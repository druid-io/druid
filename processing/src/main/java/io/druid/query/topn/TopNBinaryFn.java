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

package io.druid.query.topn;

import com.google.common.base.Function;
import com.google.common.collect.Maps;
import io.druid.granularity.AllGranularity;
import io.druid.granularity.QueryGranularity;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.guava.nary.BinaryFn;
import io.druid.query.Result;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorUtil;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.DimensionHandlerUtils;
import io.druid.segment.column.ValueType;
import org.joda.time.DateTime;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 */
public class TopNBinaryFn implements BinaryFn<Result<TopNResultValue>, Result<TopNResultValue>, Result<TopNResultValue>>
{
  private static Function<Object, Object> STRING_TRANSFORMER = new Function<Object, Object>()
  {
    @Override
    public Object apply(Object input)
    {
      if (input == null) {
        return null;
      }
      if (input instanceof String) {
        return input;
      }
      return input.toString();
    }
  };

  private static Function<Object, Object> LONG_TRANSFORMER = new Function<Object, Object>()
  {
    @Override
    public Object apply(Object input)
    {
      return DimensionHandlerUtils.convertObjectToLong(input);
    }
  };

  private static Function<Object, Object> FLOAT_TRANSFORMER = new Function<Object, Object>()
  {
    @Override
    public Object apply(Object input)
    {
      return DimensionHandlerUtils.convertObjectToFloat(input);
    }
  };

  private final TopNResultMerger merger;
  private final DimensionSpec dimSpec;
  private final QueryGranularity gran;
  private final String dimension;
  private final TopNMetricSpec topNMetricSpec;
  private final int threshold;
  private final List<AggregatorFactory> aggregations;
  private final List<PostAggregator> postAggregations;
  private final Comparator comparator;
  private final Map<String, ValueType> typeHints;
  private final Function<Object, Object> valueTransformer;

  public TopNBinaryFn(
      final TopNResultMerger merger,
      final QueryGranularity granularity,
      final DimensionSpec dimSpec,
      final TopNMetricSpec topNMetricSpec,
      final int threshold,
      final List<AggregatorFactory> aggregatorSpecs,
      final List<PostAggregator> postAggregatorSpecs,
      final Map<String, ValueType> typeHints
  )
  {
    this.merger = merger;
    this.dimSpec = dimSpec;
    this.gran = granularity;
    this.topNMetricSpec = topNMetricSpec;
    this.threshold = threshold;
    this.aggregations = aggregatorSpecs;

    this.postAggregations = AggregatorUtil.pruneDependentPostAgg(
        postAggregatorSpecs,
        topNMetricSpec.getMetricName(dimSpec)
    );

    this.dimension = dimSpec.getOutputName();
    this.comparator = topNMetricSpec.getComparator(aggregatorSpecs, postAggregatorSpecs);
    this.typeHints = typeHints;
    this.valueTransformer = initValueTransformer();
  }

  @Override
  public Result<TopNResultValue> apply(Result<TopNResultValue> arg1, Result<TopNResultValue> arg2)
  {
    if (arg1 == null) {
      return merger.getResult(convertResultType(arg2), comparator);
    }
    if (arg2 == null) {
      return merger.getResult(convertResultType(arg1), comparator);
    }

    Map<Object, DimensionAndMetricValueExtractor> retVals = new LinkedHashMap<>();

    TopNResultValue arg1Vals = arg1.getValue();
    TopNResultValue arg2Vals = arg2.getValue();

    for (DimensionAndMetricValueExtractor arg1Val : arg1Vals) {
      retVals.put(arg1Val.getDimensionValue(dimension), getTransformedExtractor(arg1Val));
    }
    for (DimensionAndMetricValueExtractor arg2Val : arg2Vals) {
      final Object dimensionValue = arg2Val.getDimensionValue(dimension);
      final Object transformedDimVal = valueTransformer.apply(dimensionValue);
      DimensionAndMetricValueExtractor arg1Val = retVals.get(transformedDimVal);

      if (arg1Val != null) {
        // size of map = aggregator + topNDim + postAgg (If sorting is done on post agg field)
        Map<String, Object> retVal = new LinkedHashMap<>(aggregations.size() + 2);

        retVal.put(dimension, transformedDimVal);
        for (AggregatorFactory factory : aggregations) {
          final String metricName = factory.getName();
          retVal.put(metricName, factory.combine(arg1Val.getMetric(metricName), arg2Val.getMetric(metricName)));
        }

        for (PostAggregator pf : postAggregations) {
          retVal.put(pf.getName(), pf.compute(retVal));
        }

        retVals.put(dimensionValue, new DimensionAndMetricValueExtractor(retVal));
      } else {
        if (dimensionValue != transformedDimVal) {
          arg2Val = getExtractorWithOverriddenValue(dimension, transformedDimVal, arg2Val);
        }
        retVals.put(dimensionValue, arg2Val);
      }
    }

    final DateTime timestamp;
    if (gran instanceof AllGranularity) {
      timestamp = arg1.getTimestamp();
    } else {
      timestamp = gran.toDateTime(gran.truncate(arg1.getTimestamp().getMillis()));
    }

    TopNResultBuilder bob = topNMetricSpec.getResultBuilder(
        timestamp,
        dimSpec,
        threshold,
        comparator,
        aggregations,
        postAggregations
    );
    for (DimensionAndMetricValueExtractor extractor : retVals.values()) {
      bob.addEntry(extractor);
    }
    return bob.build();
  }

  private DimensionAndMetricValueExtractor getExtractorWithOverriddenValue(
      String dimension,
      Object overrideVal,
      DimensionAndMetricValueExtractor originalExtractor
  )
  {
    Map<String, Object> newMap = Maps.newHashMap(originalExtractor.getBaseObject());
    newMap.put(dimension, overrideVal);
    DimensionAndMetricValueExtractor newExtractor = new DimensionAndMetricValueExtractor(newMap);
    return newExtractor;
  }

  private Function<Object, Object> initValueTransformer()
  {
    if (typeHints == null) {
      return STRING_TRANSFORMER;
    }

    ValueType type = typeHints.get(dimension);
    if (type == null) {
      type = ValueType.STRING;
    }
    switch (type) {
      case STRING:
        return STRING_TRANSFORMER;
      case LONG:
        return LONG_TRANSFORMER;
      case FLOAT:
        return FLOAT_TRANSFORMER;
      default:
        throw new IAE("invalid type");
    }
  }

  private DimensionAndMetricValueExtractor getTransformedExtractor(DimensionAndMetricValueExtractor argVal)
  {
    Object dimVal = argVal.getDimensionValue(dimension);
    Object transformedDimVal = valueTransformer.apply(dimVal);
    if (dimVal != transformedDimVal) {
      return getExtractorWithOverriddenValue(dimension, transformedDimVal, argVal);
    } else {
      return argVal;
    }
  }

  private Result<TopNResultValue> convertResultType(Result<TopNResultValue> arg1)
  {
    boolean somethingChanged = false;

    Map<Object, DimensionAndMetricValueExtractor> retVals = new LinkedHashMap<>();
    TopNResultValue arg1Vals = arg1.getValue();
    for (DimensionAndMetricValueExtractor arg1Val : arg1Vals) {
      DimensionAndMetricValueExtractor transformedExtractor = getTransformedExtractor(arg1Val);
      retVals.put(arg1Val.getDimensionValue(dimension), transformedExtractor);
      if (transformedExtractor != arg1Val) {
        somethingChanged = true;
      }
    }

    if (somethingChanged) {
      DateTime timestamp = arg1.getTimestamp();
      TopNResultBuilder bob = topNMetricSpec.getResultBuilder(
          timestamp,
          dimSpec,
          threshold,
          comparator,
          aggregations,
          postAggregations
      );
      for (DimensionAndMetricValueExtractor extractor : retVals.values()) {
        bob.addEntry(extractor);
      }
      return bob.build();
    } else {
      return arg1;
    }
  }
}
