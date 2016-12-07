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

package io.druid.segment.virtual;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Maps;
import com.google.common.primitives.Doubles;
import io.druid.common.guava.GuavaUtils;
import io.druid.math.expr.Expr;
import io.druid.math.expr.Parser;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.column.ValueType;

import java.util.Map;

public class ExpressionObjectColumnSelector implements ObjectColumnSelector<Number>
{
  private final Expr expression;
  private final Expr.ObjectBinding bindings;

  ExpressionObjectColumnSelector(Expr expression, ColumnSelectorFactory columnSelectorFactory)
  {
    this.expression = Preconditions.checkNotNull(expression, "expression");
    this.bindings = createBindings(expression, columnSelectorFactory);
  }

  private static Expr.ObjectBinding createBindings(Expr expression, ColumnSelectorFactory columnSelectorFactory)
  {
    final Map<String, Supplier<Number>> suppliers = Maps.newHashMap();
    for (String columnName : Parser.findRequiredBindings(expression)) {
      final ValueType nativeType = columnSelectorFactory.getNativeType(columnName);
      final Supplier<Number> supplier;

      if (nativeType == ValueType.FLOAT) {
        final FloatColumnSelector selector = columnSelectorFactory.makeFloatColumnSelector(columnName);
        supplier = new Supplier<Number>()
        {
          @Override
          public Number get()
          {
            return selector.get();
          }
        };
      } else if (nativeType == ValueType.LONG) {
        final LongColumnSelector selector = columnSelectorFactory.makeLongColumnSelector(columnName);
        supplier = new Supplier<Number>()
        {
          @Override
          public Number get()
          {
            return selector.get();
          }
        };
      } else if (nativeType == null) {
        // Unknown ValueType. Try making an Object selector and see if that gives us anything useful.
        final ObjectColumnSelector selector = columnSelectorFactory.makeObjectColumnSelector(columnName);
        final Class clazz = selector == null ? null : selector.classOfObject();
        if (selector == null || (clazz != Object.class && Number.class.isAssignableFrom(clazz))) {
          // We know there are no numbers here. Use a null supplier.
          supplier = null;
        } else {
          // There may be numbers here.
          supplier = new Supplier<Number>()
          {
            @Override
            public Number get()
            {
              return tryParse(selector.get());
            }
          };
        }
      } else {
        // Unhandleable ValueType (possibly STRING or COMPLEX).
        supplier = null;
      }

      if (supplier != null) {
        suppliers.put(columnName, supplier);
      }
    }

    return Parser.withSuppliers(suppliers);
  }

  private static Number tryParse(final Object value)
  {
    if (value == null) {
      return 0L;
    }

    if (value instanceof Number) {
      return (Number) value;
    }

    final String stringValue = String.valueOf(value);
    final Long longValue = GuavaUtils.tryParseLong(stringValue);
    if (longValue != null) {
      return longValue;
    }

    final Double doubleValue = Doubles.tryParse(stringValue);
    if (doubleValue != null) {
      return doubleValue;
    }

    return 0L;
  }

  @Override
  public Class<Number> classOfObject()
  {
    return Number.class;
  }

  @Override
  public Number get()
  {
    return expression.eval(bindings).numericValue();
  }
}
