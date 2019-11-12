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
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.segment.TestHelper;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

public class PartialHashSegmentMergeIngestionSpecTest
{
  private static final ObjectMapper OBJECT_MAPPER = Factory.createObjectMapper();
  private static final HashPartitionLocation HASH_PARTITION_LOCATION = new HashPartitionLocation(
      Factory.HOST,
      Factory.PORT,
      Factory.USE_HTTPS,
      Factory.SUBTASK_ID,
      Factory.INTERVAL,
      Factory.PARTITION_ID
  );
  private static final PartialHashSegmentMergeIOConfig IO_CONFIG =
      new PartialHashSegmentMergeIOConfig(Collections.singletonList(HASH_PARTITION_LOCATION));
  private static final HashedPartitionsSpec PARTITIONS_SPEC = new HashedPartitionsSpec(
      null,
      1,
      Collections.emptyList()
  );

  private PartialHashSegmentMergeIngestionSpec target;

  @Before
  public void setup()
  {
    target = new PartialHashSegmentMergeIngestionSpec(
        Factory.createDataSchema(Factory.INPUT_INTERVALS),
        IO_CONFIG,
        new Factory.TuningConfigBuilder()
            .partitionsSpec(PARTITIONS_SPEC)
            .build()
    );
  }

  @Test
  public void serializesDeserializes()
  {
    TestHelper.testSerializesDeserializes(OBJECT_MAPPER, target);
  }
}
