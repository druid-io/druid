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

package org.apache.druid.java.util.common;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class BlockingQueueHelperTest
{

  @Test
  public void testOfferingHappyPath() throws InterruptedException
  {
    final int queueCapacity = 3;
    BlockingQueue<Integer> q = new ArrayBlockingQueue<>(queueCapacity);
    BlockingQueueHelper<Integer> helper = new BlockingQueueHelper<>(q);
    for (int i = 0; i < queueCapacity; i++) {
      helper.offerAndHandleFailure(
          4,
          15L,
          TimeUnit.MILLISECONDS,
          () -> Assert.fail("Queue offer should not have timed out.")
      );
    }
    Assert.assertEquals(0, q.remainingCapacity());
  }

  @Test
  public void testOfferingFailure() throws InterruptedException
  {
    final int queueCapacity = 3;
    BlockingQueue<Integer> q = new ArrayBlockingQueue<>(queueCapacity);
    BlockingQueueHelper<Integer> helper = new BlockingQueueHelper<>(q);
    AtomicInteger lambdaCount = new AtomicInteger(0);
    for (int i = 0; i < queueCapacity + 2; i++) {
      helper.offerAndHandleFailure(
          4,
          15L,
          TimeUnit.MILLISECONDS,
          lambdaCount::incrementAndGet
      );
    }
    Assert.assertEquals(0, q.remainingCapacity());
    Assert.assertEquals(2, lambdaCount.get());
  }
}
