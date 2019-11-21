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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.timeline.partition.ShardSpec;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Statistics about a partition created by {@link PartialSegmentGenerateTask}. Each partition is a set of data
 * of the same time chunk (primary partition key) and the same {@link ShardSpec} (secondary partition key). This class
 * holds the statistics of a single partition created by a task.
 */
public class GenericPartitionStat extends PartitionStat<ShardSpec>
{
  private static final String PROP_SHARD_SPEC = "shardSpec";

  // Secondary partition key
  private final ShardSpec shardSpec;

  @JsonCreator
  public GenericPartitionStat(
      @JsonProperty("taskExecutorHost") String taskExecutorHost,
      @JsonProperty("taskExecutorPort") int taskExecutorPort,
      @JsonProperty("useHttps") boolean useHttps,
      @JsonProperty("interval") Interval interval,
      @JsonProperty(PROP_SHARD_SPEC) ShardSpec shardSpec,
      @JsonProperty("numRows") @Nullable Integer numRows,
      @JsonProperty("sizeBytes") @Nullable Long sizeBytes
  )
  {
    super(taskExecutorHost, taskExecutorPort, useHttps, interval, numRows, sizeBytes);
    this.shardSpec = shardSpec;
  }

  @Override
  public int getPartitionId()
  {
    return shardSpec.getPartitionNum();
  }

  @JsonProperty(PROP_SHARD_SPEC)
  @Override
  ShardSpec getSecondaryPartition()
  {
    return shardSpec;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    GenericPartitionStat that = (GenericPartitionStat) o;
    return Objects.equals(shardSpec, that.shardSpec);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), shardSpec);
  }
}
