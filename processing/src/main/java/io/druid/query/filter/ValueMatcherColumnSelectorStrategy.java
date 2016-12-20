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

package io.druid.query.filter;

import io.druid.query.dimension.ColumnSelectorStrategy;
import io.druid.segment.ColumnSelectorFactory;

public interface ValueMatcherColumnSelectorStrategy extends ColumnSelectorStrategy
{
  /**
   * Create a single value ValueMatcher, used for filtering by QueryableIndexStorageAdapter and FilteredAggregatorFactory.
   *
   * @param cursor ColumnSelectorFactory for creating dimension value selectors
   * @param value Value to match against
   * @return ValueMatcher that matches on 'value'
   */
  ValueMatcher getValueMatcher(ColumnSelectorFactory cursor, String value);


  /**
   * Create a predicate-based ValueMatcher, used for filtering by QueryableIndexStorageAdapter and FilteredAggregatorFactory.
   *
   * @param cursor ColumnSelectorFactory for creating dimension value selectors
   * @param predicateFactory A DruidPredicateFactory that provides the filter predicates to be matched
   * @return A ValueMatcher that applies the predicate for this DimensionQueryHelper's value type from the predicateFactory
   */
  ValueMatcher getValueMatcher(ColumnSelectorFactory cursor, final DruidPredicateFactory predicateFactory);
}
