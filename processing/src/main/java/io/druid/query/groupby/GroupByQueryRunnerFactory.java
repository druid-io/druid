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

package io.druid.query.groupby;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.guava.Sequence;
import io.druid.cache.BitmapCache;
import io.druid.cache.Cache;
import io.druid.data.input.Row;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryToolChest;
import io.druid.query.groupby.strategy.GroupByStrategySelector;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;

import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 */
public class GroupByQueryRunnerFactory implements QueryRunnerFactory<Row, GroupByQuery>
{
  private final GroupByStrategySelector strategySelector;
  private final GroupByQueryQueryToolChest toolChest;

  @BitmapCache
  @Inject(optional = true)
  private Cache cache;

  @Inject
  public GroupByQueryRunnerFactory(
      GroupByStrategySelector strategySelector,
      GroupByQueryQueryToolChest toolChest
  )
  {
    this(strategySelector, toolChest, null);
  }

  public GroupByQueryRunnerFactory(
      GroupByStrategySelector strategySelector,
      GroupByQueryQueryToolChest toolChest,
      Cache cache
  )
  {
    this.strategySelector = strategySelector;
    this.toolChest = toolChest;
    this.cache = cache;
  }

  @Override
  public QueryRunner<Row> createRunner(final Segment segment)
  {
    return new GroupByQueryRunner(segment, strategySelector, cache);
  }

  @Override
  public QueryRunner<Row> mergeRunners(final ExecutorService exec, final Iterable<QueryRunner<Row>> queryRunners)
  {
    // mergeRunners should take ListeningExecutorService at some point
    final ListeningExecutorService queryExecutor = MoreExecutors.listeningDecorator(exec);

    return new QueryRunner<Row>()
    {
      @Override
      public Sequence<Row> run(Query<Row> query, Map<String, Object> responseContext)
      {
        return strategySelector.strategize((GroupByQuery) query).mergeRunners(queryExecutor, queryRunners).run(
            query,
            responseContext
        );
      }
    };
  }

  @Override
  public QueryToolChest<Row, GroupByQuery> getToolchest()
  {
    return toolChest;
  }

  private static class GroupByQueryRunner implements QueryRunner<Row>
  {
    private final StorageAdapter adapter;
    private final GroupByStrategySelector strategySelector;
    private final Cache cache;

    public GroupByQueryRunner(Segment segment, GroupByStrategySelector strategySelector, Cache cache)
    {
      this.adapter = segment.asStorageAdapter();
      this.strategySelector = strategySelector;
      this.cache = cache;
    }

    @Override
    public Sequence<Row> run(Query<Row> input, Map<String, Object> responseContext)
    {
      if (!(input instanceof GroupByQuery)) {
        throw new ISE("Got a [%s] which isn't a %s", input.getClass(), GroupByQuery.class);
      }

      return strategySelector.strategize((GroupByQuery) input).process((GroupByQuery) input, adapter, cache);
    }
  }
}
