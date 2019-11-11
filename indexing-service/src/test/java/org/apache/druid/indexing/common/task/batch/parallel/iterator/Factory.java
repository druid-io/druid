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

package org.apache.druid.indexing.common.task.batch.parallel.iterator;

import com.google.common.base.Optional;
import org.apache.druid.data.input.Firehose;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowIterator;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

class Factory
{
  static final DateTime TIMESTAMP = new DateTime(0);
  static final String DIMENSION = "dimension";
  static final Optional<Interval> PRESENT_BUCKET_INTERVAL_OPT = Optional.of(Intervals.ETERNITY);

  static InputRow createInputRow(DateTime timestamp)
  {
    return createInputRow(timestamp, Collections.singletonList(DIMENSION));
  }

  static InputRow createInputRow(DateTime timestamp, List<String> dimensionValues)
  {
    InputRow inputRow = EasyMock.mock(InputRow.class);
    EasyMock.expect(inputRow.getTimestamp()).andStubReturn(timestamp);
    EasyMock.expect(inputRow.getDimension(DIMENSION)).andStubReturn(dimensionValues);
    EasyMock.replay(inputRow);
    return inputRow;
  }

  static Firehose createFirehose(InputRow inputRow)
  {
    Firehose firehose = EasyMock.mock(Firehose.class);
    try {
      EasyMock.expect(firehose.hasMore()).andStubReturn(true);
      EasyMock.expect(firehose.nextRow()).andStubReturn(inputRow);
    }
    catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    EasyMock.replay(firehose);

    return firehose;
  }

  static GranularitySpec createAbsentBucketIntervalGranularitySpec(DateTime timestamp)
  {
    return createGranularitySpec(timestamp, Optional.absent());
  }

  static GranularitySpec createGranularitySpec(DateTime timestamp, Optional<Interval> bucketIntervalOpt)
  {
    GranularitySpec granularitySpec = EasyMock.mock(GranularitySpec.class);
    EasyMock.expect(granularitySpec.bucketInterval(timestamp)).andStubReturn(bucketIntervalOpt);
    EasyMock.replay(granularitySpec);
    return granularitySpec;
  }

  static HandlerTester createHandlerTester(Supplier<IndexTaskInputRowIteratorBuilder> iteratorBuilderSupplier)
  {
    return new HandlerTester(iteratorBuilderSupplier);
  }

  static class HandlerTester
  {
    enum Handler
    {
      NULL_ROW,
      ABSENT_BUCKET_INTERVAL,
      APPENDED
    }

    private final Supplier<IndexTaskInputRowIteratorBuilder> iteratorBuilderSupplier;

    private HandlerTester(Supplier<IndexTaskInputRowIteratorBuilder> iteratorBuilderSupplier)
    {
      this.iteratorBuilderSupplier = iteratorBuilderSupplier;
    }

    List<Handler> invokeHandlers(
        Firehose firehose,
        GranularitySpec granularitySpec,
        InputRow expectedNextInputRow
    )
    {
      return invokeHandlers(
          firehose,
          granularitySpec,
          Collections.emptyList(),
          expectedNextInputRow
      );
    }

    List<Handler> invokeHandlers(
        Firehose firehose,
        GranularitySpec granularitySpec,
        List<InputRowIterator.InputRowHandler> appendedHandlers,
        InputRow expectedNextInputRow
    )
    {
      List<Handler> handlerInvocationHistory = new ArrayList<>();
      IndexTaskInputRowIteratorBuilder iteratorBuilder = iteratorBuilderSupplier.get()
          .firehose(firehose)
          .granularitySpec(granularitySpec)
          .nullRowRunnable(() -> handlerInvocationHistory.add(Handler.NULL_ROW))
          .absentBucketIntervalConsumer(row -> handlerInvocationHistory.add(Handler.ABSENT_BUCKET_INTERVAL));

      if (iteratorBuilder instanceof DefaultIndexTaskInputRowIteratorBuilder) {
        appendedHandlers.stream()
                        .peek(handler -> handlerInvocationHistory.add(Handler.APPENDED))
                        .forEach(((DefaultIndexTaskInputRowIteratorBuilder) iteratorBuilder)::appendInputRowHandler);
      }

      InputRowIterator iterator = iteratorBuilder.build();

      InputRow nextInputRow = iterator.next();
      Assert.assertEquals(expectedNextInputRow, nextInputRow);

      return handlerInvocationHistory;
    }
  }
}
