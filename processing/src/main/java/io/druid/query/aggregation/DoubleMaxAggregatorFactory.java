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
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Doubles;
import io.druid.common.utils.StringUtils;
import io.druid.segment.ColumnSelectorFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 */
public class DoubleMaxAggregatorFactory extends AggregatorFactory
{
  private static final byte CACHE_TYPE_ID = 0x3;

  private final String name;
  private final String fieldName;

  @JsonCreator
  public DoubleMaxAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") final String fieldName
  )
  {
    Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    Preconditions.checkNotNull(fieldName, "Must have a valid, non-null fieldName");

    this.name = name;
    this.fieldName = fieldName;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    return new DoubleMaxAggregator(metricFactory.makeFloatColumnSelector(fieldName));
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    return new DoubleMaxBufferAggregator(metricFactory.makeFloatColumnSelector(fieldName));
  }

  @Override
  public Comparator getComparator()
  {
    return DoubleMaxAggregator.COMPARATOR;
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    return DoubleMaxAggregator.combineValues(lhs, rhs);
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new DoubleMaxAggregatorFactory(name, name);
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
    return Arrays.<AggregatorFactory>asList(new DoubleMaxAggregatorFactory(fieldName, fieldName));
  }

  @Override
  public Object deserialize(Object object)
  {
    // handle "NaN" / "Infinity" values serialized as strings in JSON
    if (object instanceof String) {
      return Double.parseDouble((String) object);
    }
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

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @Override
  public List<String> requiredFields()
  {
    return ImmutableList.of(fieldName);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] fieldNameBytes = StringUtils.toUtf8WithNullToEmpty(fieldName);

    return ByteBuffer.allocate(1 + fieldNameBytes.length)
                     .put(CACHE_TYPE_ID)
                     .put(fieldNameBytes)
                     .array();
  }

  @Override
  public String getTypeName()
  {
    return "float";
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Doubles.BYTES;
  }

  @Override
  public String toString()
  {
    return "DoubleMaxAggregatorFactory{" +
           "fieldName='" + fieldName + '\'' +
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

    DoubleMaxAggregatorFactory that = (DoubleMaxAggregatorFactory) o;

    if (!Objects.equals(fieldName, that.fieldName)) {
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
    result = 31 * result + (name != null ? name.hashCode() : 0);
    return result;
  }
}
