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

package io.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.LockTryAcquireAction;
import io.druid.indexing.common.actions.SegmentListUsedAction;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.IndexSpec;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;

/**
 */
public class SameIntervalMergeTask extends AbstractFixedIntervalTask
{
  private static final Boolean defaultBuildV9Directly = Boolean.TRUE;
  private static final String TYPE = "same_interval_merge";
  @JsonIgnore
  private final List<AggregatorFactory> aggregators;
  private final Boolean rollup;
  private final IndexSpec indexSpec;
  private final Boolean buildV9Directly;

  public SameIntervalMergeTask(
      @JsonProperty("id") String id,
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("aggregations") List<AggregatorFactory> aggregators,
      @JsonProperty("rollup") Boolean rollup,
      @JsonProperty("indexSpec") IndexSpec indexSpec,
      @JsonProperty("buildV9Directly") Boolean buildV9Directly,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(
        makeId(id, TYPE, dataSource, interval),
        dataSource,
        interval,
        context
    );
    this.aggregators = Preconditions.checkNotNull(aggregators, "null aggregations");
    this.rollup = rollup == null ? Boolean.TRUE : rollup;
    this.indexSpec = indexSpec == null ? new IndexSpec() : indexSpec;
    this.buildV9Directly = buildV9Directly == null ? defaultBuildV9Directly : buildV9Directly;
  }

  @JsonProperty("aggregations")
  public List<AggregatorFactory> getAggregators()
  {
    return aggregators;
  }

  @JsonProperty
  public Boolean getRollup()
  {
    return rollup;
  }

  @JsonProperty
  public IndexSpec getIndexSpec()
  {
    return indexSpec;
  }

  @JsonProperty
  public Boolean getBuildV9Directly()
  {
    return buildV9Directly;
  }

  public static String makeId(String id, final String typeName, String dataSource, Interval interval)
  {
    return id != null ? id : joinId(
        typeName,
        dataSource,
        interval.getStart(),
        interval.getEnd(),
        new DateTime().toString()
    );
  }

  @Override
  public String getType()
  {
    return TYPE;
  }

  @Override
  public TaskStatus run(TaskToolbox toolbox) throws Exception
  {
    final List<DataSegment> segments = toolbox.getTaskActionClient().submit(
        new SegmentListUsedAction(
            getDataSource(),
            getInterval(),
            null
        )
    );
    SubTask mergeTask = new SubTask(
        getId(),
        getDataSource(),
        aggregators,
        rollup,
        indexSpec,
        buildV9Directly,
        segments,
        getContext()
    );
    final TaskStatus status = mergeTask.run(toolbox);
    if (!status.isSuccess()) {
      return TaskStatus.fromCode(getId(), status.getStatusCode());
    }
    return success();
  }

  public static class SubTask extends MergeTask
  {
    private static final String TYPE = "same_interval_merge_sub";

    @JsonCreator
    public SubTask(
        @JsonProperty("baseId") String baseId,
        @JsonProperty("dataSource") String dataSource,
        @JsonProperty("aggregations") List<AggregatorFactory> aggregators,
        @JsonProperty("rollup") Boolean rollup,
        @JsonProperty("indexSpec") IndexSpec indexSpec,
        @JsonProperty("buildV9Directly") Boolean buildV9Directly,
        @JsonProperty("segments") List<DataSegment> segments,
        @JsonProperty("context") Map<String, Object> context
    )
    {
      super(
          "sub_" + baseId,
          dataSource,
          segments,
          aggregators,
          rollup,
          indexSpec,
          buildV9Directly,
          context
      );
    }

    @Override
    public String getType()
    {
      return TYPE;
    }

    @Override
    protected void verifyInputSegments(List<DataSegment> segments)
    {
      // do nothing
    }

    @Override
    public boolean isReady(TaskActionClient taskActionClient) throws Exception
    {
      return taskActionClient.submit(new LockTryAcquireAction(getInterval())) != null;
    }
  }
}
