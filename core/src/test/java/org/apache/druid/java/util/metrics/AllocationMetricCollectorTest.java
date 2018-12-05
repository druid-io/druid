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

package org.apache.druid.java.util.metrics;

import org.apache.druid.java.util.common.logger.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;

public class AllocationMetricCollectorTest
{
  private static final Logger log = new Logger(AllocationMetricCollectorTest.class);
  private final List<Thread> threads = new ArrayList<>();
  private final int objectHeader64BitSize = 16;

  /**
   * Test a calculated delta is larger than objects size generated by this method.
   * @throws InterruptedException
   */
  @SuppressWarnings("OptionalIsPresent")
  @Test
  public void testDelta() throws InterruptedException
  {
    AllocationMetricCollector allocationMetricCollector = new AllocationMetricCollector();
    Optional<Long> delta = allocationMetricCollector.calculateDelta();
    Assert.assertNotNull(delta);
    if (delta.isPresent()) {
      Assert.assertTrue(delta.get() > 0);
      log.info("First delta: %s", delta.get());
    }
    int generatedSize2 = generateObjectsConcurrently(1000);
    Optional<Long> delta2 = allocationMetricCollector.calculateDelta();
    if (delta.isPresent()) {
      Assert.assertTrue(delta2.get() > generatedSize2);
      log.info("Second delta: %s", delta2.get());
    }

    int generatedSize3 = generateObjectsConcurrently(100000);
    Optional<Long> delta3 = allocationMetricCollector.calculateDelta();
    if (delta.isPresent()) {
      Assert.assertTrue(delta3.get() > generatedSize3);
      log.info("Third delta: %s", delta3.get());
    }
  }

  private int generateObjectsConcurrently(int countPerThread) throws InterruptedException
  {
    int threads = Math.max(1, Runtime.getRuntime().availableProcessors() - 1);
    log.info("Threads: %s", threads);
    int objectsCount = countPerThread * threads;
    log.info("Total objects: %s", objectsCount);
    int totalSize = objectsCount * objectHeader64BitSize;
    log.info("Total size: %s", totalSize);
    CountDownLatch countDownLatch = new CountDownLatch(threads);
    for (int i = 0; i < threads; i++) {
      Thread thread = new Thread(() -> {
        @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
        List<Object> list = new ArrayList<>(countPerThread);
        for (int j = 0; j < countPerThread; j++) {
          list.add(new Object());
        }
        countDownLatch.countDown();
        //noinspection EmptyCatchBlock,UnusedCatchParameter
        try {
          Thread.sleep(Long.MAX_VALUE);
        }
        catch (InterruptedException e) {
        }
      });
      thread.setDaemon(true);
      thread.start();
      this.threads.add(thread);

    }
    countDownLatch.await();
    return totalSize;
  }

  @After
  public void stopThreads()
  {
    // threads are in sleep so that their ids are still present in JVM allocation "registry"
    // so stop them manually
    for (Thread thread : threads) {
      thread.interrupt();
    }
  }
}
