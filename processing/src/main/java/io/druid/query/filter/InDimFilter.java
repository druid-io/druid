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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.google.common.primitives.Floats;
import io.druid.common.guava.GuavaUtils;
import io.druid.java.util.common.StringUtils;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.lookup.LookupExtractionFn;
import io.druid.query.lookup.LookupExtractor;
import io.druid.segment.filter.InFilter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class InDimFilter implements DimFilter
{
  // determined through benchmark that binary search on long[] is faster than HashSet until ~16 elements
  // Hashing threshold is not applied to String for now, String still uses ImmutableSortedSet
  public static final int LONG_HASHING_THRESHOLD = 16;
  public static final int FLOAT_HASHING_THRESHOLD = 16;

  private final ImmutableSortedSet<String> values;
  private final String dimension;
  private final ExtractionFn extractionFn;
  private final Supplier<DruidLongPredicate> longPredicateSupplier;
  private final Supplier<DruidFloatPredicate> floatPredicateSupplier;

  @JsonCreator
  public InDimFilter(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("values") Collection<String> values,
      @JsonProperty("extractionFn") ExtractionFn extractionFn
  )
  {
    Preconditions.checkNotNull(dimension, "dimension can not be null");
    Preconditions.checkArgument(values != null && !values.isEmpty(), "values can not be null or empty");
    this.values = ImmutableSortedSet.copyOf(
        Iterables.transform(
            values, new Function<String, String>()
            {
              @Override
              public String apply(String input)
              {
                return Strings.nullToEmpty(input);
              }

            }
        )
    );
    this.dimension = dimension;
    this.extractionFn = extractionFn;
    this.longPredicateSupplier = getLongPredicateSupplier();
    this.floatPredicateSupplier = getFloatPredicateSupplier();
  }

  @JsonProperty
  public String getDimension()
  {
    return dimension;
  }

  @JsonProperty
  public Set<String> getValues()
  {
    return values;
  }

  @JsonProperty
  public ExtractionFn getExtractionFn()
  {
    return extractionFn;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] dimensionBytes = StringUtils.toUtf8(dimension);
    final byte[][] valuesBytes = new byte[values.size()][];
    int valuesBytesSize = 0;
    int index = 0;
    for (String value : values) {
      valuesBytes[index] = StringUtils.toUtf8(Strings.nullToEmpty(value));
      valuesBytesSize += valuesBytes[index].length + 1;
      ++index;
    }
    byte[] extractionFnBytes = extractionFn == null ? new byte[0] : extractionFn.getCacheKey();

    ByteBuffer filterCacheKey = ByteBuffer.allocate(3
                                                    + dimensionBytes.length
                                                    + valuesBytesSize
                                                    + extractionFnBytes.length)
                                          .put(DimFilterUtils.IN_CACHE_ID)
                                          .put(dimensionBytes)
                                          .put(DimFilterUtils.STRING_SEPARATOR)
                                          .put(extractionFnBytes)
                                          .put(DimFilterUtils.STRING_SEPARATOR);
    for (byte[] bytes : valuesBytes) {
      filterCacheKey.put(bytes)
                    .put((byte) 0xFF);
    }
    return filterCacheKey.array();
  }

  @Override
  public DimFilter optimize()
  {
    InDimFilter inFilter = optimizeLookup();
    if (inFilter.values.size() == 1) {
      return new SelectorDimFilter(inFilter.dimension, inFilter.values.first(), inFilter.getExtractionFn());
    }
    return inFilter;
  }

  private InDimFilter optimizeLookup() {
    if (extractionFn instanceof LookupExtractionFn
        && ((LookupExtractionFn) extractionFn).isOptimize()) {
      LookupExtractionFn exFn = (LookupExtractionFn) extractionFn;
      LookupExtractor lookup = exFn.getLookup();

      final List<String> keys = new ArrayList<>();
      for (String value : values) {

        // We cannot do an unapply()-based optimization if the selector value
        // and the replaceMissingValuesWith value are the same, since we have to match on
        // all values that are not present in the lookup.
        final String convertedValue = Strings.emptyToNull(value);
        if (!exFn.isRetainMissingValue() && Objects.equals(convertedValue, exFn.getReplaceMissingValueWith())) {
          return this;
        }
        keys.addAll(lookup.unapply(convertedValue));

        // If retainMissingValues is true and the selector value is not in the lookup map,
        // there may be row values that match the selector value but are not included
        // in the lookup map. Match on the selector value as well.
        // If the selector value is overwritten in the lookup map, don't add selector value to keys.
        if (exFn.isRetainMissingValue() && lookup.apply(convertedValue) == null) {
          keys.add(convertedValue);
        }
      }

      if (keys.isEmpty()) {
        return this;
      } else {
        return new InDimFilter(dimension, keys, null);
      }
    }
    return this;
  }

  @Override
  public Filter toFilter()
  {
    return new InFilter(dimension, values, longPredicateSupplier, floatPredicateSupplier, extractionFn);
  }

  @Override
  public RangeSet<String> getDimensionRangeSet(String dimension)
  {
    if (!Objects.equals(getDimension(), dimension) || getExtractionFn() != null) {
      return null;
    }
    RangeSet<String> retSet = TreeRangeSet.create();
    for (String value : values) {
      retSet.add(Range.singleton(Strings.nullToEmpty(value)));
    }
    return retSet;
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

    InDimFilter that = (InDimFilter) o;

    if (values != null ? !values.equals(that.values) : that.values != null) {
      return false;
    }
    if (!dimension.equals(that.dimension)) {
      return false;
    }
    return extractionFn != null ? extractionFn.equals(that.extractionFn) : that.extractionFn == null;

  }

  @Override
  public int hashCode()
  {
    int result = values != null ? values.hashCode() : 0;
    result = 31 * result + dimension.hashCode();
    result = 31 * result + (extractionFn != null ? extractionFn.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    final StringBuilder builder = new StringBuilder();

    if (extractionFn != null) {
      builder.append(extractionFn).append("(");
    }

    builder.append(dimension);

    if (extractionFn != null) {
      builder.append(")");
    }

    builder.append(" IN (").append(Joiner.on(", ").join(values)).append(")");

    return builder.toString();
  }

  // As the set of filtered values can be large, parsing them as longs should be done only if needed, and only once.
  // Pass in a common long predicate supplier to all filters created by .toFilter(), so that
  // we only compute the long hashset/array once per query.
  // This supplier must be thread-safe, since this DimFilter will be accessed in the query runners.
  private Supplier<DruidLongPredicate> getLongPredicateSupplier()
  {
    return new Supplier<DruidLongPredicate>()
    {
      private final Object initLock = new Object();
      private DruidLongPredicate predicate;


      private void initLongValues()
      {
        if (predicate != null) {
          return;
        }

        synchronized (initLock) {
          if (predicate != null) {
            return;
          }

          List<Long> longs = new ArrayList<>();
          for (String value : values) {
            Long longValue = GuavaUtils.tryParseLong(value);
            if (longValue != null) {
              longs.add(longValue);
            }
          }

          if (longs.size() > LONG_HASHING_THRESHOLD) {
            final HashSet<Long> longHashSet = new HashSet<>(longs);

            predicate = new DruidLongPredicate()
            {
              @Override
              public boolean applyLong(long input)
              {
                return longHashSet.contains(input);
              }
            };
          } else {
            final long[] longArray;
            longArray = new long[longs.size()];
            for (int i = 0; i < longs.size(); i++) {
              longArray[i] = longs.get(i).longValue();
            }
            Arrays.sort(longArray);

            predicate = new DruidLongPredicate()
            {
              @Override
              public boolean applyLong(long input)
              {
                return Arrays.binarySearch(longArray, input) >= 0;
              }
            };
          }
        }
      }

      @Override
      public DruidLongPredicate get()
      {
        initLongValues();
        return predicate;
      }
    };
  }

  private Supplier<DruidFloatPredicate> getFloatPredicateSupplier()
  {
    return new Supplier<DruidFloatPredicate>()
    {
      private final Object initLock = new Object();
      private DruidFloatPredicate predicate;

      private void initFloatValues()
      {
        if (predicate != null) {
          return;
        }

        synchronized (initLock) {
          if (predicate != null) {
            return;
          }

          List<Float> floats = new ArrayList<>();
          for (String value : values) {
            Float floatValue = Floats.tryParse(value);
            if (floatValue != null) {
              floats.add(floatValue);
            }
          }

          if (floats.size() > FLOAT_HASHING_THRESHOLD) {
            final HashSet<Float> floatHashSet = new HashSet<>(floats);

            predicate = new DruidFloatPredicate()
            {
              @Override
              public boolean applyFloat(float input)
              {
                return floatHashSet.contains(input);
              }
            };
          } else {
            final float[] floatArray;
            floatArray = new float[floats.size()];
            for (int i = 0; i < floats.size(); i++) {
              floatArray[i] = floats.get(i).floatValue();
            }
            Arrays.sort(floatArray);

            predicate = new DruidFloatPredicate()
            {
              @Override
              public boolean applyFloat(float input)
              {
                return Arrays.binarySearch(floatArray, input) >= 0;
              }
            };
          }
        }
      }

      @Override
      public DruidFloatPredicate get()
      {
        initFloatValues();
        return predicate;
      }
    };
  }
}
