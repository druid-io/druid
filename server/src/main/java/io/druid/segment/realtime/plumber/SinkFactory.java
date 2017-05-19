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

package io.druid.segment.realtime.plumber;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.realtime.FireHydrant;
import io.druid.timeline.partition.ShardSpec;
import org.joda.time.Interval;

import java.util.List;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = DefaultSinkFactory.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "default", value = DefaultSinkFactory.class)
})
public interface SinkFactory
{
  public Sink create(
      Interval interval,
      DataSchema schema,
      ShardSpec shardSpec,
      String version,
      int maxRowsInMemory,
      boolean reportParseExceptions
  );

  public Sink create(
      Interval interval,
      DataSchema schema,
      ShardSpec shardSpec,
      String version,
      int maxRowsInMemory,
      boolean reportParseExceptions,
      List<FireHydrant> hydrants
  );
}
