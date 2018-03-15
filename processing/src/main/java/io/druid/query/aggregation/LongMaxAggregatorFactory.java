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

package io.druid.query.aggregation;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.druid.java.util.common.StringUtils;
import io.druid.math.expr.ExprMacroTable;
import io.druid.math.expr.Parser;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 */
public class LongMaxAggregatorFactory extends NullableAggregatorFactory
{
  private final String name;
  private final String fieldName;
  private final String expression;
  private final ExprMacroTable macroTable;

  @JsonCreator
  public LongMaxAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") final String fieldName,
      @JsonProperty("expression") String expression,
      @JacksonInject ExprMacroTable macroTable
  )
  {
    Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    Preconditions.checkArgument(
        fieldName == null ^ expression == null,
        "Must have a valid, non-null fieldName or expression"
    );

    this.name = name;
    this.fieldName = fieldName;
    this.expression = expression;
    this.macroTable = macroTable;
  }

  public LongMaxAggregatorFactory(String name, String fieldName)
  {
    this(name, fieldName, null, ExprMacroTable.nil());
  }

  @Override
  protected ColumnValueSelector selector(ColumnSelectorFactory metricFactory)
  {
    return getLongColumnSelector(metricFactory);
  }

  @Override
  protected Aggregator factorize(ColumnSelectorFactory metricFactory, ColumnValueSelector selector)
  {
    return new LongMaxAggregator(selector);
  }

  @Override
  protected BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory, ColumnValueSelector selector)
  {
    return new LongMaxBufferAggregator(selector);
  }

  private ColumnValueSelector getLongColumnSelector(ColumnSelectorFactory metricFactory)
  {
    return AggregatorUtil.makeColumnValueSelectorWithLongDefault(
        metricFactory,
        macroTable,
        fieldName,
        expression,
        Long.MIN_VALUE
    );
  }

  @Override
  public Comparator getComparator()
  {
    return LongMaxAggregator.COMPARATOR;
  }

  @Override
  @Nullable
  public Object combine(@Nullable Object lhs, @Nullable Object rhs)
  {
    if (rhs == null) {
      return lhs;
    }
    if (lhs == null) {
      return rhs;
    }
    return LongMaxAggregator.combineValues(lhs, rhs);
  }

  @Override
  public AggregateCombiner makeAggregateCombiner2()
  {
    return new LongAggregateCombiner()
    {
      private long max;

      @Override
      public void reset(ColumnValueSelector selector)
      {
        max = selector.getLong();
      }

      @Override
      public void fold(ColumnValueSelector selector)
      {
        max = Math.max(max, selector.getLong());
      }

      @Override
      public long getLong()
      {
        return max;
      }
    };
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new LongMaxAggregatorFactory(name, name, null, macroTable);
  }

  @Override
  public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException
  {
    if (other.getName().equals(this.getName()) && this.getClass() == other.getClass()) {
      return getCombiningFactory();
    } else {
      throw new AggregatorFactoryNotMergeableException(this, other);
    }
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Arrays.<AggregatorFactory>asList(new LongMaxAggregatorFactory(fieldName, fieldName, expression, macroTable));
  }

  @Override
  public Object deserialize(Object object)
  {
    return object;
  }

  @Override
  @Nullable
  public Object finalizeComputation(@Nullable Object object)
  {
    return object;
  }

  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @JsonProperty
  public String getExpression()
  {
    return expression;
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @Override
  public List<String> requiredFields()
  {
    return fieldName != null
           ? Collections.singletonList(fieldName)
           : Parser.findRequiredBindings(Parser.parse(expression, macroTable));
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] fieldNameBytes = StringUtils.toUtf8WithNullToEmpty(fieldName);
    byte[] expressionBytes = StringUtils.toUtf8WithNullToEmpty(expression);

    return ByteBuffer.allocate(2 + fieldNameBytes.length + expressionBytes.length)
                     .put(AggregatorUtil.LONG_MAX_CACHE_TYPE_ID)
                     .put(fieldNameBytes)
                     .put(AggregatorUtil.STRING_SEPARATOR)
                     .put(expressionBytes)
                     .array();
  }

  @Override
  public String getTypeName()
  {
    return "long";
  }

  @Override
  public int getMaxIntermediateSize2()
  {
    return Long.BYTES;
  }

  @Override
  public String toString()
  {
    return "LongMaxAggregatorFactory{" +
           "fieldName='" + fieldName + '\'' +
           ", expression='" + expression + '\'' +
           ", name='" + name + '\'' +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    LongMaxAggregatorFactory that = (LongMaxAggregatorFactory) o;

    if (!Objects.equals(fieldName, that.fieldName)) {
      return false;
    }
    if (!Objects.equals(expression, that.expression)) {
      return false;
    }
    if (!Objects.equals(name, that.name)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = fieldName != null ? fieldName.hashCode() : 0;
    result = 31 * result + (expression != null ? expression.hashCode() : 0);
    result = 31 * result + (name != null ? name.hashCode() : 0);
    return result;
  }
}
