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

package org.apache.druid.indexing.common.task.batch.partition;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpec;
import org.joda.time.Interval;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class HashPartitionAnalysis implements CompletePartitionAnalysis<Integer, HashedPartitionsSpec>
{
  /**
   * Key is the time ranges for the primary partitioning.
   * Value is the number of partitions per time range for the secondary partitioning
   */
  private final Map<Interval, Integer> intervalToNumBuckets = new HashMap<>();
  private final HashedPartitionsSpec partitionsSpec;

  public HashPartitionAnalysis(HashedPartitionsSpec partitionsSpec)
  {
    this.partitionsSpec = partitionsSpec;
  }

  @Override
  public HashedPartitionsSpec getPartitionsSpec()
  {
    return partitionsSpec;
  }

  @Override
  public void updateBucket(Interval interval, Integer bucketAnalysis)
  {
    intervalToNumBuckets.put(interval, bucketAnalysis);
  }

  @Override
  public Integer getBucketAnalysis(Interval interval)
  {
    return Preconditions.checkNotNull(
        intervalToNumBuckets.get(interval),
        "Missing numBuckets for interval[%s]",
        interval
    );
  }

  @Override
  public Set<Interval> getAllIntervalsToIndex()
  {
    return Collections.unmodifiableSet(intervalToNumBuckets.keySet());
  }

  @Override
  public int numTimePartitions()
  {
    return intervalToNumBuckets.size();
  }

  public void forEach(BiConsumer<Interval, Integer> consumer)
  {
    intervalToNumBuckets.forEach(consumer);
  }

  @Override
  public Map<Interval, List<SegmentIdWithShardSpec>> convertToIntervalToSegmentIds(
      TaskToolbox toolbox,
      String dataSource,
      Function<Interval, String> versionFinder
  )
  {
    final Map<Interval, List<SegmentIdWithShardSpec>> intervalToSegmentIds =
        Maps.newHashMapWithExpectedSize(numTimePartitions());

    forEach((interval, numBuckets) -> {
      intervalToSegmentIds.put(
          interval,
          IntStream.range(0, numBuckets)
                   .mapToObj(i -> {
                     final HashBasedNumberedShardSpec shardSpec = new HashBasedNumberedShardSpec(
                         i,
                         numBuckets,
                         partitionsSpec.getPartitionDimensions(),
                         toolbox.getJsonMapper()
                     );
                     return new SegmentIdWithShardSpec(
                         dataSource,
                         interval,
                         versionFinder.apply(interval),
                         shardSpec
                     );
                   })
                   .collect(Collectors.toList())
      );
    });

    return intervalToSegmentIds;
  }
}
