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

package org.apache.druid.query.aggregation.cardinality.accurate;


import org.apache.druid.query.ColumnSelectorPlus;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.cardinality.accurate.collector.Collector;
import org.apache.druid.query.aggregation.cardinality.accurate.types.AccurateCardinalityAggregatorColumnSelectorStrategy;

import javax.annotation.Nullable;

public class AccurateCardinalityAggregator implements Aggregator
{
  private final String name;
  private final ColumnSelectorPlus<AccurateCardinalityAggregatorColumnSelectorStrategy> selectorPlus;
  private final Collector collector;

  public AccurateCardinalityAggregator(
      String name,
      ColumnSelectorPlus<AccurateCardinalityAggregatorColumnSelectorStrategy> selectorPlus,
      Collector collector
  )
  {
    this.name = name;
    this.selectorPlus = selectorPlus;
    this.collector = collector;
  }

  @Override
  public void aggregate()
  {
    collector.add(selectorPlus.getColumnSelectorStrategy().getUniversalUniqueCode(selectorPlus.getSelector()));
  }

  @Nullable
  @Override
  public Object get()
  {
    return collector;
  }

  @Override
  public float getFloat()
  {
    throw new UnsupportedOperationException("AccurateCardinalityAggregator does not support getFloat()");
  }

  @Override
  public long getLong()
  {
    throw new UnsupportedOperationException("AccurateCardinalityAggregator does not support getLong()");
  }

  @Override
  public Aggregator clone()
  {
    return new AccurateCardinalityAggregator(name, selectorPlus, collector);
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }

}


