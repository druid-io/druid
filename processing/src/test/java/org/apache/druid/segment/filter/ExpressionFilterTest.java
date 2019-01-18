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

package org.apache.druid.segment.filter;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.FloatDimensionSchema;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimeAndDimsParseSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.ExpressionDimFilter;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Closeable;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RunWith(Parameterized.class)
public class ExpressionFilterTest extends BaseFilterTest
{
  private static final String TIMESTAMP_COLUMN = "timestamp";

  private static final InputRowParser<Map<String, Object>> PARSER = new MapInputRowParser(
      new TimeAndDimsParseSpec(
          new TimestampSpec(TIMESTAMP_COLUMN, "iso", DateTimes.of("2000"), null),
          new DimensionsSpec(
              ImmutableList.of(
                  new StringDimensionSchema("dim0"),
                  new LongDimensionSchema("dim1"),
                  new FloatDimensionSchema("dim2"),
                  new StringDimensionSchema("dim3"),
                  new StringDimensionSchema("dim4")
              ),
              null,
              null
          )
      )
  );

  private static final List<InputRow> ROWS = ImmutableList.<Map<String, Object>>of(
      ImmutableMap.of("dim0", "0", "dim1", 0L, "dim2", 0.0f, "dim3", "", "dim4", ImmutableList.of("1", "2")),
      ImmutableMap.of("dim0", "1", "dim1", 1L, "dim2", 1.0f, "dim3", "10", "dim4", ImmutableList.of()),
      ImmutableMap.of("dim0", "2", "dim1", 2L, "dim2", 2.0f, "dim3", "2", "dim4", ImmutableList.of("")),
      ImmutableMap.of("dim0", "3", "dim1", 3L, "dim2", 3.0f, "dim3", "1", "dim4", ImmutableList.of("3")),
      ImmutableMap.of("dim0", "4", "dim1", 4L, "dim2", 4.0f, "dim3", "1", "dim4", ImmutableList.of("4", "5")),
      ImmutableMap.of("dim0", "5", "dim1", 5L, "dim2", 5.0f, "dim3", "5", "dim4", ImmutableList.of("4", "5")),
      ImmutableMap.of("dim0", "6", "dim1", 6L, "dim2", 6.0f, "dim3", "1"),
      ImmutableMap.of("dim0", "7", "dim1", 7L, "dim2", 7.0f, "dim3", "a"),
      ImmutableMap.of("dim0", "8", "dim1", 8L, "dim2", 8.0f, "dim3", 8L),
      ImmutableMap.of("dim0", "9", "dim1", 9L, "dim2", 9.0f, "dim3", 1.234f, "dim4", 1.234f)
  ).stream().map(e -> PARSER.parseBatch(e).get(0)).collect(Collectors.toList());

  public ExpressionFilterTest(
      String testName,
      IndexBuilder indexBuilder,
      Function<IndexBuilder, Pair<StorageAdapter, Closeable>> finisher,
      boolean cnf,
      boolean optimize
  )
  {
    super(
        testName,
        ROWS,
        indexBuilder.schema(
            new IncrementalIndexSchema.Builder()
                .withDimensionsSpec(PARSER.getParseSpec().getDimensionsSpec()).build()
        ),
        finisher,
        cnf,
        optimize
    );
  }

  @AfterClass
  public static void tearDown() throws Exception
  {
    BaseFilterTest.tearDown(ColumnComparisonFilterTest.class.getName());
  }

  @Test
  public void testOneSingleValuedStringColumn()
  {
    assertFilterMatches(EDF("dim3 == ''"), ImmutableList.of("0"));
    assertFilterMatches(EDF("dim3 == '1'"), ImmutableList.of("3", "4", "6"));
    assertFilterMatches(EDF("dim3 == 'a'"), ImmutableList.of("7"));
    assertFilterMatches(EDF("dim3 == 1"), ImmutableList.of("3", "4", "6"));
    assertFilterMatches(EDF("dim3 == 1.0"), ImmutableList.of("3", "4", "6"));
    assertFilterMatches(EDF("dim3 == 1.234"), ImmutableList.of("9"));
    assertFilterMatches(EDF("dim3 < '2'"), ImmutableList.of("0", "1", "3", "4", "6", "9"));
    if (NullHandling.replaceWithDefault()) {
      assertFilterMatches(EDF("dim3 < 2"), ImmutableList.of("0", "3", "4", "6", "7", "9"));
      assertFilterMatches(EDF("dim3 < 2.0"), ImmutableList.of("0", "3", "4", "6", "7", "9"));
    } else {
      // Empty String and "a" will not match
      assertFilterMatches(EDF("dim3 < 2"), ImmutableList.of("3", "4", "6", "9"));
      assertFilterMatches(EDF("dim3 < 2.0"), ImmutableList.of("3", "4", "6", "9"));
    }
    assertFilterMatches(EDF("like(dim3, '1%')"), ImmutableList.of("1", "3", "4", "6", "9"));
  }

  @Test
  public void testOneMultiValuedStringColumn()
  {
    // Expressions currently treat multi-valued arrays as nulls.
    // This test is just documenting the current behavior, not necessarily saying it makes sense.
    if (NullHandling.replaceWithDefault()) {
      assertFilterMatches(EDF("dim4 == ''"), ImmutableList.of("0", "1", "2", "4", "5", "6", "7", "8"));
    } else {
      assertFilterMatches(EDF("dim4 == ''"), ImmutableList.of("2"));
      // AS per SQL standard null == null returns false.
      assertFilterMatches(EDF("dim4 == null"), ImmutableList.of());
    }
    assertFilterMatches(EDF("dim4 == '1'"), ImmutableList.of());
    assertFilterMatches(EDF("dim4 == '3'"), ImmutableList.of("3"));
  }

  @Test
  public void testOneLongColumn()
  {
    if (NullHandling.replaceWithDefault()) {
      assertFilterMatches(EDF("dim1 == ''"), ImmutableList.of("0"));
    } else {
      // A long does not match empty string
      assertFilterMatches(EDF("dim1 == ''"), ImmutableList.of());
    }
    assertFilterMatches(EDF("dim1 == '1'"), ImmutableList.of("1"));
    assertFilterMatches(EDF("dim1 == 2"), ImmutableList.of("2"));
    assertFilterMatches(EDF("dim1 < '2'"), ImmutableList.of("0", "1"));
    assertFilterMatches(EDF("dim1 < 2"), ImmutableList.of("0", "1"));
    assertFilterMatches(EDF("dim1 < 2.0"), ImmutableList.of("0", "1"));
    assertFilterMatches(EDF("like(dim1, '1%')"), ImmutableList.of("1"));
  }

  @Test
  public void testOneFloatColumn()
  {
    if (NullHandling.replaceWithDefault()) {
      assertFilterMatches(EDF("dim2 == ''"), ImmutableList.of("0"));
    } else {
      // A float does not match empty string
      assertFilterMatches(EDF("dim2 == ''"), ImmutableList.of());
    }
    assertFilterMatches(EDF("dim2 == '1'"), ImmutableList.of("1"));
    assertFilterMatches(EDF("dim2 == 2"), ImmutableList.of("2"));
    assertFilterMatches(EDF("dim2 < '2'"), ImmutableList.of("0", "1"));
    assertFilterMatches(EDF("dim2 < 2"), ImmutableList.of("0", "1"));
    assertFilterMatches(EDF("dim2 < 2.0"), ImmutableList.of("0", "1"));
    assertFilterMatches(EDF("like(dim2, '1%')"), ImmutableList.of("1"));
  }

  @Test
  public void testConstantExpression()
  {
    assertFilterMatches(EDF("1 + 1"), ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"));
    assertFilterMatches(EDF("0 + 0"), ImmutableList.of());
  }

  @Test
  public void testCompareColumns()
  {
    // String vs string
    assertFilterMatches(EDF("dim0 == dim3"), ImmutableList.of("2", "5", "8"));

    if (NullHandling.replaceWithDefault()) {
      // String vs long
      assertFilterMatches(EDF("dim1 == dim3"), ImmutableList.of("0", "2", "5", "8"));

      // String vs float
      assertFilterMatches(EDF("dim2 == dim3"), ImmutableList.of("0", "2", "5", "8"));
    } else {
      // String vs long
      assertFilterMatches(EDF("dim1 == dim3"), ImmutableList.of("2", "5", "8"));

      // String vs float
      assertFilterMatches(EDF("dim2 == dim3"), ImmutableList.of("2", "5", "8"));
    }

    // String vs. multi-value string
    // Expressions currently treat multi-valued arrays as nulls.
    // This test is just documenting the current behavior, not necessarily saying it makes sense.
    assertFilterMatches(EDF("dim0 == dim4"), ImmutableList.of("3"));
  }

  @Test
  public void testMissingColumn()
  {
    if (NullHandling.replaceWithDefault()) {
      assertFilterMatches(EDF("missing == ''"), ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"));
    } else {
      // AS per SQL standard null == null returns false.
      assertFilterMatches(EDF("missing == null"), ImmutableList.of());
    }
    assertFilterMatches(EDF("missing == '1'"), ImmutableList.of());
    assertFilterMatches(EDF("missing == 2"), ImmutableList.of());
    if (NullHandling.replaceWithDefault()) {
      // missing equivaluent to 0
      assertFilterMatches(EDF("missing < '2'"), ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"));
      assertFilterMatches(EDF("missing < 2"), ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"));
      assertFilterMatches(EDF("missing < 2.0"), ImmutableList.of("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"));
    } else {
      // missing equivalent to null
      assertFilterMatches(EDF("missing < '2'"), ImmutableList.of());
      assertFilterMatches(EDF("missing < 2"), ImmutableList.of());
      assertFilterMatches(EDF("missing < 2.0"), ImmutableList.of());
    }
    assertFilterMatches(EDF("missing > '2'"), ImmutableList.of());
    assertFilterMatches(EDF("missing > 2"), ImmutableList.of());
    assertFilterMatches(EDF("missing > 2.0"), ImmutableList.of());
    assertFilterMatches(EDF("like(missing, '1%')"), ImmutableList.of());
  }

  @Test
  public void testGetRequiredColumn()
  {
    Assert.assertEquals(EDF("like(dim1, '1%')").getRequiredColumns(), Sets.newHashSet("dim1"));
    Assert.assertEquals(EDF("dim2 == '1'").getRequiredColumns(), Sets.newHashSet("dim2"));
    Assert.assertEquals(EDF("dim3 < '2'").getRequiredColumns(), Sets.newHashSet("dim3"));
    Assert.assertEquals(EDF("dim4 == ''").getRequiredColumns(), Sets.newHashSet("dim4"));
    Assert.assertEquals(EDF("1 + 1").getRequiredColumns(), new HashSet<>());
    Assert.assertEquals(EDF("dim0 == dim3").getRequiredColumns(), Sets.newHashSet("dim0", "dim3"));
    Assert.assertEquals(EDF("missing == ''").getRequiredColumns(), Sets.newHashSet("missing"));
  }

  private static ExpressionDimFilter EDF(final String expression)
  {
    return new ExpressionDimFilter(expression, TestExprMacroTable.INSTANCE);
  }
}
