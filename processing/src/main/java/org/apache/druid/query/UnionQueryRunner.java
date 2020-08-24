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

package org.apache.druid.query;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.guava.MergeSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.planning.DataSourceAnalysis;

public class UnionQueryRunner<T> implements QueryRunner<T>
{
  private final QueryRunner<T> baseRunner;

  public UnionQueryRunner(
      QueryRunner<T> baseRunner
  )
  {
    this.baseRunner = baseRunner;
  }

  @Override
  public Sequence<T> run(final QueryPlus<T> queryPlus, final ResponseContext responseContext)
  {
    Query<T> query = queryPlus.getQuery();

    final DataSourceAnalysis analysis = DataSourceAnalysis.forDataSource(query.getDataSource());

    if (analysis.isConcreteTableBased() && analysis.getBaseTableDataSources().get().size() != 1) {
      // Union of tables.

      return new MergeSequence<>(
          query.getResultOrdering(),
          Sequences.simple(
              Lists.transform(
                  analysis.getBaseTableDataSources().get(),
                  (Function<DataSource, Sequence<T>>) singleSource ->
                      baseRunner.run(
                          queryPlus.withQuery(
                              Queries.withBaseDataSource(query, singleSource)
                                     // assign the subqueryId. this will be used to validate that every query servers
                                     // have responded per subquery in RetryQueryRunner
                                     .withDefaultSubQueryId()
                          ),
                          responseContext
                      )
              )
          )
      );
    } else {
      return baseRunner.run(queryPlus, responseContext);
    }
  }

}
