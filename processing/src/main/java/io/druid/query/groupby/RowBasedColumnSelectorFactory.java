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

import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.druid.data.input.Row;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.math.expr.Parser;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.NumericColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ColumnCapabilitiesImpl;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.IndexedInts;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntIterators;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class RowBasedColumnSelectorFactory implements ColumnSelectorFactory
{
  private final Supplier<? extends Row> row;
  private final Map<String, ValueType> columnTypes;

  public RowBasedColumnSelectorFactory(final Supplier<? extends Row> row, final Map<String, ValueType> columnTypes)
  {
    this.row = row;
    this.columnTypes = columnTypes != null ? columnTypes : ImmutableMap.<String, ValueType>of();
  }

  public static RowBasedColumnSelectorFactory create(
      final Supplier<? extends Row> row,
      final Map<String, ValueType> columnTypes
  )
  {
    return new RowBasedColumnSelectorFactory(row, columnTypes);
  }

  public static RowBasedColumnSelectorFactory create(
      final ThreadLocal<? extends Row> row,
      final Map<String, ValueType> columnTypes
  )
  {
    return new RowBasedColumnSelectorFactory(
        new Supplier<Row>()
        {
          @Override
          public Row get()
          {
            return row.get();
          }
        },
        columnTypes
    );
  }

  // This dimension selector does not have an associated lookup dictionary, which means lookup can only be done
  // on the same row. This dimension selector is used for applying the extraction function on dimension, which
  // requires a DimensionSelector implementation
  @Override
  public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
  {
    return dimensionSpec.decorate(makeDimensionSelectorUndecorated(dimensionSpec));
  }

  private DimensionSelector makeDimensionSelectorUndecorated(DimensionSpec dimensionSpec)
  {
    final String dimension = dimensionSpec.getDimension();
    final ExtractionFn extractionFn = dimensionSpec.getExtractionFn();
    final boolean isTimeColumn = dimensionSpec.getDimension().equals(Column.TIME_COLUMN_NAME);

    if (isTimeColumn && extractionFn == null) {
      throw new UnsupportedOperationException("time dimension must provide an extraction function");
    }

    return new DimensionSelector()
    {
      @Override
      public IndexedInts getRow()
      {
        final int dimensionValuesSize;

        if (isTimeColumn) {
          dimensionValuesSize = 1;
        } else {
          final List<String> dimensionValues = row.get().getDimension(dimension);
          dimensionValuesSize = dimensionValues != null ? dimensionValues.size() : 0;
        }

        return new IndexedInts()
        {
          @Override
          public int size()
          {
            return dimensionValuesSize;
          }

          @Override
          public int get(int index)
          {
            if (index < 0 || index >= dimensionValuesSize) {
              throw new IndexOutOfBoundsException("index: " + index);
            }
            return index;
          }

          @Override
          public IntIterator iterator()
          {
            return IntIterators.fromTo(0, dimensionValuesSize);
          }

          @Override
          public void close() throws IOException
          {

          }

          @Override
          public void fill(int index, int[] toFill)
          {
            throw new UnsupportedOperationException("fill not supported");
          }
        };
      }

      @Override
      public int getValueCardinality()
      {
        return DimensionSelector.CARDINALITY_UNKNOWN;
      }

      @Override
      public String lookupName(int id)
      {
        if (isTimeColumn) {
          return extractionFn.apply(row.get().getTimestampFromEpoch());
        } else {
          final String value = Strings.emptyToNull(row.get().getDimension(dimension).get(id));
          return extractionFn == null ? value : extractionFn.apply(value);
        }
      }

      @Override
      public int lookupId(String name)
      {
        throw new UnsupportedOperationException("cannot lookup names to ids");
      }
    };
  }

  @Override
  public FloatColumnSelector makeFloatColumnSelector(final String columnName)
  {
    if (columnName.equals(Column.TIME_COLUMN_NAME)) {
      return new FloatColumnSelector()
      {
        @Override
        public float get()
        {
          return (float) row.get().getTimestampFromEpoch();
        }
      };
    } else {
      return new FloatColumnSelector()
      {
        @Override
        public float get()
        {
          return row.get().getFloatMetric(columnName);
        }
      };
    }
  }

  @Override
  public LongColumnSelector makeLongColumnSelector(final String columnName)
  {
    if (columnName.equals(Column.TIME_COLUMN_NAME)) {
      return new LongColumnSelector()
      {
        @Override
        public long get()
        {
          return row.get().getTimestampFromEpoch();
        }
      };
    } else {
      return new LongColumnSelector()
      {
        @Override
        public long get()
        {
          return row.get().getLongMetric(columnName);
        }
      };
    }
  }

  @Override
  public ObjectColumnSelector makeObjectColumnSelector(final String columnName)
  {
    if (columnName.equals(Column.TIME_COLUMN_NAME)) {
      return new ObjectColumnSelector()
      {
        @Override
        public Class classOfObject()
        {
          return Long.class;
        }

        @Override
        public Object get()
        {
          return row.get().getTimestampFromEpoch();
        }
      };
    } else {
      return new ObjectColumnSelector()
      {
        @Override
        public Class classOfObject()
        {
          return Object.class;
        }

        @Override
        public Object get()
        {
          return row.get().getRaw(columnName);
        }
      };
    }
  }

  @Override
  public NumericColumnSelector makeMathExpressionSelector(String expression)
  {
    final Expr parsed = Parser.parse(expression);

    final List<String> required = Parser.findRequiredBindings(parsed);
    final Map<String, Supplier<Number>> values = Maps.newHashMapWithExpectedSize(required.size());

    for (final String columnName : required) {
      values.put(
          columnName, new Supplier<Number>()
          {
            @Override
            public Number get()
            {
              return Evals.toNumber(row.get().getRaw(columnName));
            }
          }
      );
    }
    final Expr.ObjectBinding binding = Parser.withSuppliers(values);

    return new NumericColumnSelector()
    {
      @Override
      public Number get()
      {
        return parsed.eval(binding).numericValue();
      }
    };
  }

  @Override
  public ColumnCapabilities getColumnCapabilities(String columnName)
  {
    if (Column.TIME_COLUMN_NAME.equals(columnName)) {
      // TIME_COLUMN_NAME is handled specially; override the provided columnTypes.
      return new ColumnCapabilitiesImpl().setType(ValueType.LONG);
    } else {
      final ValueType valueType = columnTypes.get(columnName);

      // Do _not_ set isDictionaryEncoded or hasBitmapIndexes, because Row-based columns do not have those things.
      return valueType != null
             ? new ColumnCapabilitiesImpl().setType(valueType)
             : new ColumnCapabilitiesImpl().setType(ValueType.STRING);
    }
  }
}
