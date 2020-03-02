/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.server;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.ListenableFuture;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadConfig;
import io.github.resilience4j.bulkhead.BulkheadRegistry;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import org.apache.druid.client.SegmentServerSelector;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryWatcher;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;


/**
 * QueryScheduler (potentially) assigns any {@link Query} that is to be executed to a 'query lane' using the
 * {@link QueryLaningStrategy} that is defined in {@link QuerySchedulerConfig}.
 *
 * As a {@link QueryWatcher}, it also provides cancellation facilities.
 */
public class QueryScheduler implements QueryWatcher
{
  private static final String TOTAL = "default";
  private final QueryLaningStrategy laningStrategy;
  private final BulkheadRegistry laneRegistry;

  private final SetMultimap<String, ListenableFuture<?>> queryFutures;
  private final SetMultimap<String, String> queryDatasources;

  private final AtomicLong totalAcquired = new AtomicLong();
  private final AtomicLong totalReleased = new AtomicLong();

  public QueryScheduler(int totalNumThreads, QueryLaningStrategy laningStrategy)
  {
    this.laningStrategy = laningStrategy;
    this.laneRegistry = BulkheadRegistry.of(getLaneConfigs(totalNumThreads));
    this.queryFutures = Multimaps.synchronizedSetMultimap(HashMultimap.create());
    this.queryDatasources = Multimaps.synchronizedSetMultimap(HashMultimap.create());
  }

  @Override
  public void registerQueryFuture(Query<?> query, ListenableFuture<?> future)
  {
    final String id = query.getId();
    final Set<String> datasources = query.getDataSource().getTableNames();
    queryFutures.put(id, future);
    queryDatasources.putAll(id, datasources);
    future.addListener(
        () -> {
          queryFutures.remove(id, future);
          for (String datasource : datasources) {
            queryDatasources.remove(id, datasource);
          }
        },
        Execs.directExecutor()
    );
  }

  /**
   * Assign a query a lane (if not set)
   */
  public <T> Query<T> laneQuery(QueryPlus<T> queryPlus, Set<SegmentServerSelector> segments)
  {
    Query<T> query = queryPlus.getQuery();
    // man wins over machine.. for now.
    if (QueryContexts.getLane(query) != null) {
      return query;
    }
    Optional<String> lane = laningStrategy.computeLane(queryPlus, segments);
    return lane.map(query::withLane).orElse(query);
  }

  /**
   * Run a query with the scheduler, attempting to acquire a semaphore from the total and lane specific query capacities
   */
  public <T> Sequence<T> run(Query<?> query, Sequence<T> resultSequence)
  {
    List<Bulkhead> bulkheads = acquireLanes(query);
    return resultSequence.withBaggage(() -> releaseLanes(bulkheads));
  }

  /**
   * Forcibly cancel all futures that have been registered to a specific query id
   */
  public boolean cancelQuery(String id)
  {
    // if you re-use queryId and cancel queries... you are going to have a bad time
    queryDatasources.removeAll(id);
    Set<ListenableFuture<?>> futures = queryFutures.removeAll(id);
    boolean success = true;
    for (ListenableFuture<?> future : futures) {
      success = success && future.cancel(true);
    }
    return success;
  }

  public Set<String> getQueryDatasources(final String queryId)
  {
    return queryDatasources.get(queryId);
  }

  public int getTotalAvailableCapacity()
  {
    return laneRegistry.getConfiguration(TOTAL)
                       .map(config -> laneRegistry.bulkhead(TOTAL, config).getMetrics().getAvailableConcurrentCalls())
                       .orElse(-1);
  }

  public int getLaneAvailableCapacity(String lane)
  {
    return laneRegistry.getConfiguration(lane)
                       .map(config -> laneRegistry.bulkhead(lane, config).getMetrics().getAvailableConcurrentCalls())
                       .orElse(-1);
  }

  public long getTotalAcquired()
  {
    return totalAcquired.get();
  }

  public long getTotalReleased()
  {
    return totalReleased.get();
  }

  private List<Bulkhead> acquireLanes(
      Query<?> query
  )
  {
    final String lane = QueryContexts.getLane(query);
    final Optional<BulkheadConfig> laneConfig = lane == null ? Optional.empty() : laneRegistry.getConfiguration(lane);
    List<Bulkhead> hallPasses = new ArrayList<>(2);
    // everyone needs to take one from the total lane
    final Optional<BulkheadConfig> totalConfig = laneRegistry.getConfiguration(TOTAL);
    totalConfig.ifPresent(config -> hallPasses.add(acquireTotal(config)));
    // catch the 2nd so we can release the first
    try {
      // if we have a lane, also get it
      laneConfig.ifPresent(config -> hallPasses.add(acquireLane(lane, config)));
    }
    catch (QueryCapacityExceededException ex) {
      // release total if couldn't get lane
      releaseLanes(hallPasses);
      throw ex;
    }
    return hallPasses;
  }

  private Bulkhead acquireTotal(BulkheadConfig config)
  {
    Bulkhead totalLimiter = laneRegistry.bulkhead(TOTAL, config);
    if (!totalLimiter.tryAcquirePermission()) {
      throw new QueryCapacityExceededException();
    }
    totalAcquired.incrementAndGet();
    return totalLimiter;
  }

  private Bulkhead acquireLane(String lane, BulkheadConfig config)
  {
    Bulkhead laneLimiter = laneRegistry.bulkhead(lane, config);
    if (!laneLimiter.tryAcquirePermission()) {
      throw new QueryCapacityExceededException(lane);
    }
    return laneLimiter;
  }

  private void releaseLanes(List<Bulkhead> bulkheads)
  {
    bulkheads.forEach(Bulkhead::releasePermission);
    totalReleased.incrementAndGet();
  }

  private Map<String, BulkheadConfig> getLaneConfigs(int totalNumThreads)
  {
    Map<String, BulkheadConfig> configs = new HashMap<>();
    if (totalNumThreads > 0) {
      configs.put(
          TOTAL,
          BulkheadConfig.custom().maxConcurrentCalls(totalNumThreads).maxWaitDuration(Duration.ZERO).build()
      );
    }
    for (Object2IntMap.Entry<String> entry : laningStrategy.getLaneLimits().object2IntEntrySet()) {
      configs.put(
          entry.getKey(),
          BulkheadConfig.custom().maxConcurrentCalls(entry.getIntValue()).maxWaitDuration(Duration.ZERO).build()
      );
    }
    return configs;
  }
}
