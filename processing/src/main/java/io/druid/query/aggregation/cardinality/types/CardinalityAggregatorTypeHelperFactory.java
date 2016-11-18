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

package io.druid.query.aggregation.cardinality.types;

import io.druid.java.util.common.IAE;
import io.druid.query.dimension.QueryTypeHelperFactory;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ValueType;

public class CardinalityAggregatorTypeHelperFactory implements QueryTypeHelperFactory<CardinalityAggregatorTypeHelper>
{
  @Override
  public CardinalityAggregatorTypeHelper makeQueryTypeHelper(
      String dimName, ColumnCapabilities capabilities
  )
  {
    ValueType type = capabilities.getType();
    switch(type) {
      case STRING:
        return new StringCardinalityAggregatorTypeHelper();
      default:
        throw new IAE("Cannot create query type helper from invalid type [%s]", type);
    }
  }
}
