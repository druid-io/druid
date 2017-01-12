/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.lookup;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.metamx.emitter.EmittingLogger;
import io.druid.concurrent.Execs;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.annotations.Json;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This class provide a basic {@link LookupExtractorFactory} references manager.
 * It allows basic operations fetching, listing, adding and deleting of {@link LookupExtractor} objects
 * It is be used by queries to fetch the lookup reference.
 * It is used by Lookup configuration manager to add/remove or list lookups configuration via HTTP or other protocols.
 * It does periodic snap shot of the list of lookup in order to bootstrap nodes after restart.
 */
@ManageLifecycle
public class LookupReferencesManager
{
  private static final EmittingLogger LOG = new EmittingLogger(LookupReferencesManager.class);

  private final ReadWriteLock startStopLock = new ReentrantReadWriteLock(true);

  @VisibleForTesting
  volatile boolean started = false;

  @GuardedBy("startStopLock")
  private final Map<String, LookupExtractorFactoryContainer> lookupMap = new HashMap<>();

  private final LookupSnapshotTaker lookupSnapshotTaker;

  @VisibleForTesting
  final BlockingQueue<Notice> queue = new ArrayBlockingQueue<>(10000);

  private volatile ExecutorService exec;

  //for unit testing only
  private final boolean testMode;

  @Inject
  public LookupReferencesManager(LookupConfig lookupConfig, @Json ObjectMapper objectMapper)
  {
    this(lookupConfig, objectMapper, false);
  }

  @VisibleForTesting
  LookupReferencesManager(LookupConfig lookupConfig, ObjectMapper objectMapper, boolean testMode)
  {
    if (Strings.isNullOrEmpty(lookupConfig.getSnapshotWorkingDir())) {
      this.lookupSnapshotTaker = null;
    } else {
      this.lookupSnapshotTaker = new LookupSnapshotTaker(objectMapper, lookupConfig.getSnapshotWorkingDir());
    }
    this.testMode = testMode;
  }

  @LifecycleStart
  public void start()
  {
    startStopLock.writeLock().lock();

    try {
      if (!started) {
        LOG.info("LookupReferencesManager is starting.");

        if (lookupSnapshotTaker != null) {
          final List<LookupBean> lookupBeanList = lookupSnapshotTaker.pullExistingSnapshot();
          for (LookupBean lookupBean : lookupBeanList) {
            LookupExtractorFactoryContainer container = lookupBean.container;

            //for backward compatibility with druid ver <= 0.9.2 persisted snapshots
            if (lookupBean.container == null) {
              container = new LookupExtractorFactoryContainer(null, lookupBean.factory);
            }

            if (container.getLookupExtractorFactory().start()) {
              lookupMap.put(lookupBean.name, container);
            } else {
              throw new ISE("Failed to start lookup [%s]:[%s]", lookupBean.name, container);
            }
          }
        }

        if (exec == null && !testMode) {
          exec = Execs.singleThreaded("lookup-reference-manager-%d");
          exec.execute(
              new Runnable()
              {
                @Override
                public void run()
                {
                  try {
                    while (started && !Thread.currentThread().isInterrupted()) {
                      try {
                        queue.take().handle();
                      }
                      catch (InterruptedException ex) {
                        LOG.warn("interrupted, going down... lookups are not managed anymore");
                        Thread.currentThread().interrupt();
                      }
                      catch (Exception ex) {
                        LOG.makeAlert(ex, "Exception occured while lookup notice handling.").emit();
                      }
                      catch (Throwable t) {
                        LOG.makeAlert(t, "Fatal error occured while lookup notice handling.").emit();
                        throw t;
                      }
                    }
                  }
                  finally {
                    LOG.info("Lookup Mgmt loop exited, Lookup notices are not handled anymore.");
                  }
                }
              }
          );
        }

        started = true;

        LOG.info("LookupReferencesManager is started.");
      }
    } finally {
      startStopLock.writeLock().unlock();
    }
  }

  @LifecycleStop
  public void stop()
  {
    startStopLock.writeLock().lock();

    if (started) {
      try {
        LOG.info("LookupReferencesManager is stopping.");
        started = false;

        if (exec != null) {
          exec.shutdownNow();
          exec = null;
        }

        for (Map.Entry<String, LookupExtractorFactoryContainer> e : lookupMap.entrySet()) {
          try {
            LOG.info("Closing lookup [%s]", e.getKey());
            if (!e.getValue().getLookupExtractorFactory().close()) {
              LOG.error("Failed to close lookup [%s].");
            }
          }
          catch (Exception ex) {
            LOG.error(ex, "Failed to close lookup [%s].", e.getKey());
          }
        }

        lookupMap.clear();
      }
      finally {
        startStopLock.writeLock().unlock();
      }
      LOG.info("LookupReferencesManager is stopped.");
    } else {
      startStopLock.writeLock().unlock();
    }
  }

  public void add(String lookupName, LookupExtractorFactoryContainer lookupExtractorFactoryContainer)
  {
    assertStarted();

    try {
      if (!queue.offer(new LoadNotice(lookupName, lookupExtractorFactoryContainer), 1, TimeUnit.MILLISECONDS)) {
        throw new ISE("notice queue add timedout to add [%s] lookup drop notice", lookupName);
      }
    } catch (InterruptedException ex) {
      throw new ISE(ex, "failed to add [%s] lookup load notice", lookupName);
    }
  }

  public void remove(String lookupName)
  {
    assertStarted();

    try {
      if (!queue.offer(new DropNotice(lookupName), 1, TimeUnit.MILLISECONDS)) {
        throw new ISE("notice queue add timedout to add [%s] lookup drop notice", lookupName);
      }
    } catch (InterruptedException ex) {
      throw new ISE(ex, "failed to add [%s] lookup drop notice", lookupName);
    }
  }

  @Nullable
  public LookupExtractorFactoryContainer get(String lookupName)
  {
    assertStarted();

    startStopLock.readLock().lock();
    try {
      return lookupMap.get(lookupName);
    } finally {
      startStopLock.readLock().unlock();
    }
  }

  public LookupsState getAllLookupsState()
  {
    assertStarted();

    startStopLock.readLock().lock();
    try {
      Map<String, LookupExtractorFactoryContainer> lookupsToLoad = new HashMap<>();
      Set<String> lookupsToDrop = new HashSet<>();

      Iterator<Notice> iter = queue.iterator();
      while (iter.hasNext()) {
        Notice notice = iter.next();
        if (notice instanceof LoadNotice) {
          LoadNotice loadNotice = (LoadNotice) notice;
          lookupsToLoad.put(loadNotice.lookupName, loadNotice.lookupExtractorFactoryContainer);
          lookupsToDrop.remove(loadNotice.lookupName);
        } else if (notice instanceof DropNotice) {
          DropNotice dropNotice = (DropNotice) notice;
          lookupsToDrop.add(dropNotice.lookupName);
          lookupsToLoad.remove(dropNotice.lookupName);
        } else {
          throw new ISE("Unknown Notice type [%s].", notice.getClass().getName());
        }
      }

      return new LookupsState(Maps.newHashMap(lookupMap), lookupsToLoad, lookupsToDrop);
    } finally {
      startStopLock.readLock().unlock();
    }
  }

  private void takeSnapshot()
  {
    if (lookupSnapshotTaker != null) {
      startStopLock.readLock().lock();

      List<LookupBean> lookups;
      try {
        lookups = Lists.newArrayList(
            Collections2.transform(
                lookupMap.entrySet(),
                new Function<Map.Entry<String, LookupExtractorFactoryContainer>, LookupBean>()
                {
                  @Nullable
                  @Override
                  public LookupBean apply(
                      @Nullable
                      Map.Entry<String, LookupExtractorFactoryContainer> input
                  )
                  {
                    final LookupBean lookupBean = new LookupBean();
                    lookupBean.container = input.getValue();
                    lookupBean.name = input.getKey();
                    return lookupBean;
                  }
                }
            )
        );
      }
      finally {
        startStopLock.readLock().unlock();
      }

      lookupSnapshotTaker.takeSnapshot(lookups);
    }
  }

  private void assertStarted()
  {
    if (!started) {
      throw new ISE("LookupReferencesManager is not started.");
    }
  }

  @VisibleForTesting
  interface Notice
  {
    void handle();
  }

  private class LoadNotice implements Notice
  {
    String lookupName;
    LookupExtractorFactoryContainer lookupExtractorFactoryContainer;

    public LoadNotice(String lookupName, LookupExtractorFactoryContainer lookupExtractorFactoryContainer)
    {
      this.lookupName = lookupName;
      this.lookupExtractorFactoryContainer = lookupExtractorFactoryContainer;
    }

    @Override
    public void handle()
    {
      startStopLock.readLock().lock();

      try {
        LookupExtractorFactoryContainer old = lookupMap.get(lookupName);
        if (old != null && !lookupExtractorFactoryContainer.replaces(old)) {
          LOG.warn(
              "got notice to load lookup [%s] that can't replace existing [%s].",
              lookupExtractorFactoryContainer,
              old
          );
          return;
        }
      } finally {
        startStopLock.readLock().unlock();
      }

      if (!lookupExtractorFactoryContainer.getLookupExtractorFactory().start()) {
        throw new ISE("start method returned false for lookup [%s]:[%s]", lookupName, lookupExtractorFactoryContainer);
      }

      startStopLock.writeLock().lock();
      final LookupExtractorFactoryContainer old;
      try {
        assertStarted();
        old = lookupMap.put(lookupName, lookupExtractorFactoryContainer);
      } finally {
        startStopLock.writeLock().unlock();
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Loaded lookup [%s] with spec [%s].", lookupName, lookupExtractorFactoryContainer);
      }

      takeSnapshot();

      if (old != null) {
        if (!old.getLookupExtractorFactory().close()) {
          throw new ISE("close method returned false for lookup [%s]:[%s]", lookupName, old);
        }
      }
    }
  }

  private class DropNotice implements Notice
  {
    String lookupName;

    public DropNotice(String lookupName)
    {
      this.lookupName = lookupName;
    }

    @Override
    public void handle()
    {
      startStopLock.writeLock().lock();

      final LookupExtractorFactoryContainer lookupExtractorFactoryContainer;

      try {
        assertStarted();
        lookupExtractorFactoryContainer = lookupMap.remove(lookupName);
      } finally {
        startStopLock.writeLock().unlock();
      }

      if (lookupExtractorFactoryContainer != null) {
        takeSnapshot();

        if (LOG.isDebugEnabled()) {
          LOG.debug("Removed lookup [%s] with spec [%s].", lookupName, lookupExtractorFactoryContainer);
        }

        if (!lookupExtractorFactoryContainer.getLookupExtractorFactory().close()) {
          throw new ISE("close method returned false for lookup [%s]:[%s]", lookupName, lookupExtractorFactoryContainer);
        }
      }
    }
  }
}
