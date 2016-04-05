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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import io.druid.common.utils.StringUtils;
import io.druid.math.expr.Parser;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.LongColumnSelector;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 */
public class LongMaxAggregatorFactory extends AggregatorFactory
{
  private static final byte CACHE_TYPE_ID = 0xA;

  private final String name;
  private final String fieldName;
  private final String fieldExpression;

  @JsonCreator
  public LongMaxAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") final String fieldName,
      @JsonProperty("fieldExpression") String fieldExpression
  )
  {
    Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    Preconditions.checkArgument(
        fieldName == null ^ fieldExpression == null,
        "Must have a valid, non-null fieldName or fieldExpression");

    this.name = name;
    this.fieldName = fieldName;
    this.fieldExpression = fieldExpression;
  }

  public LongMaxAggregatorFactory(String name, String fieldName)
  {
    this(name, fieldName, null);
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    return new LongMaxAggregator(name, getLongColumnSelector(metricFactory));
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    return new LongMaxBufferAggregator(getLongColumnSelector(metricFactory));
  }

  private LongColumnSelector getLongColumnSelector(ColumnSelectorFactory metricFactory)
  {
    return AggregatorUtil.getLongColumnSelector(metricFactory, fieldName, fieldExpression);
  }

  @Override
  public Comparator getComparator()
  {
    return LongMaxAggregator.COMPARATOR;
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    return LongMaxAggregator.combineValues(lhs, rhs);
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new LongMaxAggregatorFactory(name, name, null);
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
    return Arrays.<AggregatorFactory>asList(new LongMaxAggregatorFactory(fieldName, fieldName, fieldExpression));
  }

  @Override
  public Object deserialize(Object object)
  {
    return object;
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return object;
  }

  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @JsonProperty
  public String getFieldExpression()
  {
    return fieldExpression;
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
    return fieldName != null ? Arrays.asList(fieldName) : Parser.findRequiredBindings(fieldExpression);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] fieldNameBytes = StringUtils.toUtf8WithNullToEmpty(fieldName);
    byte[] fieldExpressionBytes = StringUtils.toUtf8WithNullToEmpty(fieldExpression);

    return ByteBuffer.allocate(1 + fieldNameBytes.length + fieldExpressionBytes.length)
                     .put(CACHE_TYPE_ID).put(fieldNameBytes).put(fieldExpressionBytes).array();
  }

  @Override
  public String getTypeName()
  {
    return "long";
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Longs.BYTES;
  }

  @Override
  public Object getAggregatorStartValue()
  {
    return Long.MIN_VALUE;
  }

  @Override
  public String toString()
  {
    return "LongMaxAggregatorFactory{" +
           "fieldName='" + fieldName + '\'' +
           ", fieldExpression='" + fieldExpression + '\'' +
           ", name='" + name + '\'' +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    LongMaxAggregatorFactory that = (LongMaxAggregatorFactory) o;

    if (!Objects.equals(fieldName, that.fieldName)) {
      return false;
    }
    if (!Objects.equals(fieldExpression, that.fieldExpression)) {
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
    result = 31 * result + (fieldExpression != null ? fieldExpression.hashCode() : 0);
    result = 31 * result + (name != null ? name.hashCode() : 0);
    return result;
  }
}
