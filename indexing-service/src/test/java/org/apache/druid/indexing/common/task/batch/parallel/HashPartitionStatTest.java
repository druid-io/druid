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

package org.apache.druid.indexing.common.task.batch.parallel;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class HashPartitionStatTest
{
  private static final ObjectMapper OBJECT_MAPPER = Factory.createObjectMapper();

  private HashPartitionStat target;

  @Before
  public void setup()
  {
    target = new HashPartitionStat(
        Factory.TASK_EXECUTOR_HOST,
        Factory.TASK_EXECUTOR_PORT,
        Factory.USE_HTTPS,
        Factory.INTERVAL,
        Factory.PARTITION_ID,
        Factory.NUM_ROWS,
        Factory.SIZE_BYTES
    );
  }

  @Test
  public void serializesDeserializes()
  {
    TestHelper.testSerializesDeserializes(OBJECT_MAPPER, target);
  }

  @Test
  public void hasPartitionIdThatMatchesSecondaryPartition()
  {
    Assert.assertEquals(target.getSecondaryPartition().intValue(), target.getPartitionId());
  }
}
