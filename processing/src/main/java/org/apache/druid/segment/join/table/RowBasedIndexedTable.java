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

package org.apache.druid.segment.join.table;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntLists;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.RowAdapter;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * An IndexedTable composed of a List-based table and Map-based indexes. The implementation is agnostic to the
 * specific row type; it uses a {@link RowAdapter} to work with any sort of object.
 * The class allows passing in a cache key. If the key is non-null, results of any join on this table can be cached.
 * That cache becomes invalidated if this key changes. Creators of this class can pass in a non-null cache key if its
 * possible to construct a small identifier
 *  - that must change when contents of this indexed table chances
 *  - May remain unchanged when contents of this indexed table
 *
 *  How the cache key is constructed itself, depends on how the RowBasedIndexedTable is being built.
 */
public class RowBasedIndexedTable<RowType> implements IndexedTable
{
  private final List<RowType> table;
  private final List<Map<Object, IntList>> index;
  private final RowSignature rowSignature;
  private final List<Function<RowType, Object>> columnFunctions;
  private final Set<String> keyColumns;
  private final String version;
  @Nullable
  private final byte[] cacheKey;

  public RowBasedIndexedTable(
      final List<RowType> table,
      final RowAdapter<RowType> rowAdapter,
      final RowSignature rowSignature,
      final Set<String> keyColumns,
      final String version
  )
  {
    this(table, rowAdapter, rowSignature, keyColumns, version, null);
  }

  public RowBasedIndexedTable(
      final List<RowType> table,
      final RowAdapter<RowType> rowAdapter,
      final RowSignature rowSignature,
      final Set<String> keyColumns,
      final String version,
      @Nullable
      final byte[] cacheKey
  )
  {
    this.table = table;
    this.rowSignature = rowSignature;
    this.columnFunctions =
        rowSignature.getColumnNames().stream().map(rowAdapter::columnFunction).collect(Collectors.toList());
    this.keyColumns = keyColumns;
    this.version = version;
    this.cacheKey = cacheKey;

    if (!ImmutableSet.copyOf(rowSignature.getColumnNames()).containsAll(keyColumns)) {
      throw new ISE(
          "keyColumns[%s] must all be contained in rowSignature[%s]",
          String.join(", ", keyColumns),
          rowSignature
      );
    }

    index = new ArrayList<>(rowSignature.size());

    for (int i = 0; i < rowSignature.size(); i++) {
      final String column = rowSignature.getColumnName(i);
      final Map<Object, IntList> m;

      if (keyColumns.contains(column)) {
        final ValueType keyType =
            rowSignature.getColumnType(column).orElse(IndexedTableJoinMatcher.DEFAULT_KEY_TYPE);

        final Function<RowType, Object> columnFunction = columnFunctions.get(i);

        m = new HashMap<>();

        for (int j = 0; j < table.size(); j++) {
          final RowType row = table.get(j);
          final Object key = DimensionHandlerUtils.convertObjectToType(columnFunction.apply(row), keyType);
          if (key != null) {
            final IntList array = m.computeIfAbsent(key, k -> new IntArrayList());
            array.add(j);
          }
        }
      } else {
        m = null;
      }

      index.add(m);
    }
  }

  @Override
  public String version()
  {
    return version;
  }

  @Override
  public Set<String> keyColumns()
  {
    return keyColumns;
  }

  @Override
  public RowSignature rowSignature()
  {
    return rowSignature;
  }

  @Override
  public Index columnIndex(int column)
  {
    return getKeyColumnIndex(column, index, rowSignature);
  }

  @Override
  public Reader columnReader(int column)
  {
    final Function<RowType, Object> columnFn = columnFunctions.get(column);

    if (columnFn == null) {
      throw new IAE("Column[%d] is not a valid column", column);
    }

    return row -> columnFn.apply(table.get(row));
  }

  @Override
  public byte[] computeCacheKey()
  {
    return Preconditions.checkNotNull(cacheKey, "Cache key can't be null");
  }

  @Override
  public boolean isCacheable()
  {
    return (null != cacheKey);
  }

  @Override
  public int numRows()
  {
    return table.size();
  }

  @Override
  public Optional<Closeable> acquireReferences()
  {
    // nothing to close by default, whatever loaded this thing (probably) lives on heap
    return Optional.of(() -> {});
  }

  @Override
  public void close()
  {
    // nothing to close
  }

  static Index getKeyColumnIndex(int column, List<Map<Object, IntList>> keyColumnsIndex, RowSignature rowSignature)
  {
    final Map<Object, IntList> indexMap = keyColumnsIndex.get(column);

    if (indexMap == null) {
      throw new IAE("Column[%d] is not a key column", column);
    }

    final ValueType columnType =
        rowSignature.getColumnType(column).orElse(IndexedTableJoinMatcher.DEFAULT_KEY_TYPE);

    return key -> {
      final Object convertedKey = DimensionHandlerUtils.convertObjectToType(key, columnType, false);

      if (convertedKey != null) {
        final IntList found = indexMap.get(convertedKey);
        if (found != null) {
          return found;
        } else {
          return IntLists.EMPTY_LIST;
        }
      } else {
        return IntLists.EMPTY_LIST;
      }
    };
  }
}
