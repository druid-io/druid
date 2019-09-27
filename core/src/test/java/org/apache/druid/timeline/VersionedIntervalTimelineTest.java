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

package org.apache.druid.timeline;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.timeline.partition.ImmutablePartitionHolder;
import org.apache.druid.timeline.partition.IntegerPartitionChunk;
import org.apache.druid.timeline.partition.NumberedOverwritingPartitionChunk;
import org.apache.druid.timeline.partition.NumberedPartitionChunk;
import org.apache.druid.timeline.partition.OvershadowableInteger;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.apache.druid.timeline.partition.PartitionHolder;
import org.apache.druid.timeline.partition.PartitionIds;
import org.apache.druid.timeline.partition.SingleElementPartitionChunk;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 */
public class VersionedIntervalTimelineTest
{
  VersionedIntervalTimeline<String, OvershadowableInteger> timeline;

  @Before
  public void setUp() {
    timeline = makeStringIntegerTimeline();
  }

  @Test
  public void testFindEntryWithOverlap()
  {
    add("2011-01-01/2011-01-10", "1", 1);
    add("2011-01-02/2011-01-05", "2", 1);

    Assert.assertEquals(
        new ImmutablePartitionHolder<>(new PartitionHolder<>(makeSingle("1", 1))),
        timeline.findEntry(Intervals.of("2011-01-02T02/2011-01-04"), "1")
    );
  }

  //   1|----|
  //      1|----|
  @Test(expected = UnsupportedOperationException.class)
  public void testOverlapSameVersionThrowException()
  {
    add("2011-01-01/2011-01-10", "1", 1);
    add("2011-01-05/2011-01-15", "1", 3);
  }

  //   2|----|
  //   2|----|
  //   1|----|
  //   1|----|
  @Test
  public void testOverlapSameVersionIsOkay()
  {
    add("2011-01-01/2011-01-10", "1", 1);
    add("2011-01-01/2011-01-10", "2", 2);
    add("2011-01-01/2011-01-10", "2", 3);
    add("2011-01-01/2011-01-10", "1", 4);

    assertValues(
        Collections.singletonList(
            createExpected("2011-01-01/2011-01-10", "2", 2)
        ),
        timeline.lookup(Intervals.of("2011-01-01/2011-01-10"))
    );
  }

  // 1|----|----|
  //   2|----|
  @Test
  public void testOverlapSecondBetween()
  {
    add("2011-01-01/2011-01-10", "1", 1);
    add("2011-01-10/2011-01-20", "1", 2);
    add("2011-01-05/2011-01-15", "2", 3);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-05", "1", 1),
            createExpected("2011-01-05/2011-01-15", "2", 3),
            createExpected("2011-01-15/2011-01-20", "1", 2)
        ),
        timeline.lookup(Intervals.of("2011-01-01/2011-01-20"))
    );
  }

  //   2|----|
  // 1|----|----|
  @Test
  public void testOverlapFirstBetween()
  {
    add("2011-01-05/2011-01-15", "2", 3);
    add("2011-01-01/2011-01-10", "1", 1);
    add("2011-01-10/2011-01-20", "1", 2);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-05", "1", 1),
            createExpected("2011-01-05/2011-01-15", "2", 3),
            createExpected("2011-01-15/2011-01-20", "1", 2)
        ),
        timeline.lookup(Intervals.of("2011-01-01/2011-01-20"))
    );
  }

  //   1|----|
  //      2|----|
  @Test
  public void testOverlapFirstBefore()
  {
    add("2011-01-01/2011-01-10", "1", 1);
    add("2011-01-05/2011-01-15", "2", 3);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-05", "1", 1),
            createExpected("2011-01-05/2011-01-15", "2", 3)
        ),
        timeline.lookup(Intervals.of("2011-01-01/2011-01-15"))
    );
  }

  //      2|----|
  //   1|----|
  @Test
  public void testOverlapFirstAfter()
  {
    add("2011-01-05/2011-01-15", "2", 3);
    add("2011-01-01/2011-01-10", "1", 1);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-05", "1", 1),
            createExpected("2011-01-05/2011-01-15", "2", 3)
        ),
        timeline.lookup(Intervals.of("2011-01-01/2011-01-15"))
    );
  }

  //   1|----|
  // 2|----|
  @Test
  public void testOverlapSecondBefore()
  {
    add("2011-01-05/2011-01-15", "1", 3);
    add("2011-01-01/2011-01-10", "2", 1);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-10", "2", 1),
            createExpected("2011-01-10/2011-01-15", "1", 3)
        ),
        timeline.lookup(Intervals.of("2011-01-01/2011-01-15"))
    );
  }

  // 2|----|
  //   1|----|
  @Test
  public void testOverlapSecondAfter()
  {
    add("2011-01-01/2011-01-10", "2", 3);
    add("2011-01-05/2011-01-15", "1", 1);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-10", "2", 3),
            createExpected("2011-01-10/2011-01-15", "1", 1)
        ),
        timeline.lookup(Intervals.of("2011-01-01/2011-01-15"))
    );
  }

  //   1|----------|
  //      2|----|
  @Test
  public void testOverlapFirstLarger()
  {
    add("2011-01-01/2011-01-20", "1", 2);
    add("2011-01-05/2011-01-15", "2", 3);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-05", "1", 2),
            createExpected("2011-01-05/2011-01-15", "2", 3),
            createExpected("2011-01-15/2011-01-20", "1", 2)
        ),
        timeline.lookup(Intervals.of("2011-01-01/2011-01-20"))
    );
  }

  //      2|----|
  //   1|----------|
  @Test
  public void testOverlapSecondLarger()
  {
    add("2011-01-05/2011-01-15", "2", 3);
    add("2011-01-01/2011-01-20", "1", 2);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-05", "1", 2),
            createExpected("2011-01-05/2011-01-15", "2", 3),
            createExpected("2011-01-15/2011-01-20", "1", 2)
        ),
        timeline.lookup(Intervals.of("2011-01-01/2011-01-20"))
    );
  }

  //   1|----|-----|
  //   2|-------|
  @Test
  public void testOverlapSecondPartialAlign()
  {
    add("2011-01-01/2011-01-10", "1", 1);
    add("2011-01-10/2011-01-20", "1", 2);
    add("2011-01-01/2011-01-15", "2", 3);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-15", "2", 3),
            createExpected("2011-01-15/2011-01-20", "1", 2)
        ),
        timeline.lookup(Intervals.of("2011-01-01/2011-01-20"))
    );
  }

  //   2|-------|
  //   1|----|-----|
  @Test
  public void testOverlapFirstPartialAlign()
  {
    add("2011-01-01/2011-01-15", "2", 3);
    add("2011-01-01/2011-01-10", "1", 1);
    add("2011-01-10/2011-01-20", "1", 2);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-15", "2", 3),
            createExpected("2011-01-15/2011-01-20", "1", 2)
        ),
        timeline.lookup(Intervals.of("2011-01-01/2011-01-20"))
    );
  }

  //   1|-------|
  //       2|------------|
  //     3|---|
  @Test
  public void testOverlapAscending()
  {
    add("2011-01-01/2011-01-10", "1", 1);
    add("2011-01-05/2011-01-20", "2", 2);
    add("2011-01-03/2011-01-06", "3", 3);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-03", "1", 1),
            createExpected("2011-01-03/2011-01-06", "3", 3),
            createExpected("2011-01-06/2011-01-20", "2", 2)
        ),
        timeline.lookup(Intervals.of("2011-01-01/2011-01-20"))
    );
  }

  //     3|---|
  //       2|------------|
  //   1|-------|
  @Test
  public void testOverlapDescending()
  {
    add("2011-01-03/2011-01-06", "3", 3);
    add("2011-01-05/2011-01-20", "2", 2);
    add("2011-01-01/2011-01-10", "1", 1);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-03", "1", 1),
            createExpected("2011-01-03/2011-01-06", "3", 3),
            createExpected("2011-01-06/2011-01-20", "2", 2)
        ),
        timeline.lookup(Intervals.of("2011-01-01/2011-01-20"))
    );
  }

  //       2|------------|
  //     3|---|
  //   1|-------|
  @Test
  public void testOverlapMixed()
  {
    add("2011-01-05/2011-01-20", "2", 2);
    add("2011-01-03/2011-01-06", "3", 3);
    add("2011-01-01/2011-01-10", "1", 1);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-03", "1", 1),
            createExpected("2011-01-03/2011-01-06", "3", 3),
            createExpected("2011-01-06/2011-01-20", "2", 2)
        ),
        timeline.lookup(Intervals.of("2011-01-01/2011-01-20"))
    );
  }

  //    1|-------------|
  //      2|--------|
  //      3|-----|
  @Test
  public void testOverlapContainedAscending()
  {
    add("2011-01-01/2011-01-20", "1", 1);
    add("2011-01-02/2011-01-10", "2", 2);
    add("2011-01-02/2011-01-06", "3", 3);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-02", "1", 1),
            createExpected("2011-01-02/2011-01-06", "3", 3),
            createExpected("2011-01-06/2011-01-10", "2", 2),
            createExpected("2011-01-10/2011-01-20", "1", 1)
        ),
        timeline.lookup(Intervals.of("2011-01-01/2011-01-20"))
    );
  }

  //      3|-----|
  //      2|--------|
  //    1|-------------|
  @Test
  public void testOverlapContainedDescending()
  {
    add("2011-01-02/2011-01-06", "3", 3);
    add("2011-01-02/2011-01-10", "2", 2);
    add("2011-01-01/2011-01-20", "1", 1);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-02", "1", 1),
            createExpected("2011-01-02/2011-01-06", "3", 3),
            createExpected("2011-01-06/2011-01-10", "2", 2),
            createExpected("2011-01-10/2011-01-20", "1", 1)
        ),
        timeline.lookup(Intervals.of("2011-01-01/2011-01-20"))
    );
  }

  //      2|--------|
  //      3|-----|
  //    1|-------------|
  @Test
  public void testOverlapContainedmixed()
  {
    add("2011-01-02/2011-01-10", "2", 2);
    add("2011-01-02/2011-01-06", "3", 3);
    add("2011-01-01/2011-01-20", "1", 1);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-02", "1", 1),
            createExpected("2011-01-02/2011-01-06", "3", 3),
            createExpected("2011-01-06/2011-01-10", "2", 2),
            createExpected("2011-01-10/2011-01-20", "1", 1)
        ),
        timeline.lookup(Intervals.of("2011-01-01/2011-01-20"))
    );
  }

  //      1|------|------|----|
  //                 2|-----|
  @Test
  public void testOverlapSecondContained()
  {
    add("2011-01-01/2011-01-07", "1", 1);
    add("2011-01-07/2011-01-15", "1", 2);
    add("2011-01-15/2011-01-20", "1", 3);
    add("2011-01-10/2011-01-13", "2", 4);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-07", "1", 1),
            createExpected("2011-01-07/2011-01-10", "1", 2),
            createExpected("2011-01-10/2011-01-13", "2", 4),
            createExpected("2011-01-13/2011-01-15", "1", 2),
            createExpected("2011-01-15/2011-01-20", "1", 3)
        ),
        timeline.lookup(Intervals.of("2011-01-01/2011-01-20"))
    );
  }

  //                 2|-----|
  //      1|------|------|----|
  @Test
  public void testOverlapFirstContained()
  {
    add("2011-01-10/2011-01-13", "2", 4);
    add("2011-01-01/2011-01-07", "1", 1);
    add("2011-01-07/2011-01-15", "1", 2);
    add("2011-01-15/2011-01-20", "1", 3);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-07", "1", 1),
            createExpected("2011-01-07/2011-01-10", "1", 2),
            createExpected("2011-01-10/2011-01-13", "2", 4),
            createExpected("2011-01-13/2011-01-15", "1", 2),
            createExpected("2011-01-15/2011-01-20", "1", 3)
        ),
        timeline.lookup(Intervals.of("2011-01-01/2011-01-20"))
    );
  }

  //  1|----|----|
  //  2|---------|
  @Test
  public void testOverlapSecondContainsFirst()
  {
    add("2011-01-01/2011-01-20", "1", 1);
    add("2011-01-01/2011-01-10", "2", 2);
    add("2011-01-10/2011-01-20", "2", 3);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-10", "2", 2),
            createExpected("2011-01-10/2011-01-20", "2", 3)
        ),
        timeline.lookup(Intervals.of("2011-01-01/2011-01-20"))
    );
  }

  //  2|---------|
  //  1|----|----|
  @Test
  public void testOverlapFirstContainsSecond()
  {
    add("2011-01-10/2011-01-20", "2", 3);
    add("2011-01-01/2011-01-20", "1", 1);
    add("2011-01-01/2011-01-10", "2", 2);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-10", "2", 2),
            createExpected("2011-01-10/2011-01-20", "2", 3)
        ),
        timeline.lookup(Intervals.of("2011-01-01/2011-01-20"))
    );
  }

  //  1|----|
  //     2|----|
  //        3|----|
  @Test
  public void testOverlapLayeredAscending()
  {
    add("2011-01-01/2011-01-10", "1", 1);
    add("2011-01-05/2011-01-15", "2", 2);
    add("2011-01-15/2011-01-25", "3", 3);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-05", "1", 1),
            createExpected("2011-01-05/2011-01-15", "2", 2),
            createExpected("2011-01-15/2011-01-25", "3", 3)
        ),
        timeline.lookup(Intervals.of("2011-01-01/2011-01-25"))
    );
  }

  //        3|----|
  //     2|----|
  //  1|----|
  @Test
  public void testOverlapLayeredDescending()
  {
    add("2011-01-15/2011-01-25", "3", 3);
    add("2011-01-05/2011-01-15", "2", 2);
    add("2011-01-01/2011-01-10", "1", 1);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-05", "1", 1),
            createExpected("2011-01-05/2011-01-15", "2", 2),
            createExpected("2011-01-15/2011-01-25", "3", 3)
        ),
        timeline.lookup(Intervals.of("2011-01-01/2011-01-25"))
    );
  }

  //  2|----|     |----|
  // 1|-------------|
  @Test
  public void testOverlapV1Large()
  {
    add("2011-01-01/2011-01-15", "1", 1);
    add("2011-01-03/2011-01-05", "2", 2);
    add("2011-01-13/2011-01-20", "2", 3);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-03", "1", 1),
            createExpected("2011-01-03/2011-01-05", "2", 2),
            createExpected("2011-01-05/2011-01-13", "1", 1),
            createExpected("2011-01-13/2011-01-20", "2", 3)
        ),
        timeline.lookup(Intervals.of("2011-01-01/2011-01-20"))
    );
  }

  // 2|-------------|
  //  1|----|     |----|
  @Test
  public void testOverlapV2Large()
  {
    add("2011-01-01/2011-01-15", "2", 1);
    add("2011-01-03/2011-01-05", "1", 2);
    add("2011-01-13/2011-01-20", "1", 3);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-15", "2", 1),
            createExpected("2011-01-15/2011-01-20", "1", 3)
        ),
        timeline.lookup(Intervals.of("2011-01-01/2011-01-20"))
    );
  }

  //    1|-------------|
  //  2|----|   |----|
  @Test
  public void testOverlapV1LargeIsAfter()
  {
    add("2011-01-03/2011-01-20", "1", 1);
    add("2011-01-01/2011-01-05", "2", 2);
    add("2011-01-13/2011-01-17", "2", 3);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-05", "2", 2),
            createExpected("2011-01-05/2011-01-13", "1", 1),
            createExpected("2011-01-13/2011-01-17", "2", 3),
            createExpected("2011-01-17/2011-01-20", "1", 1)
        ),
        timeline.lookup(Intervals.of("2011-01-01/2011-01-20"))
    );
  }

  //  2|----|   |----|
  //    1|-------------|
  @Test
  public void testOverlapV1SecondLargeIsAfter()
  {
    add("2011-01-13/2011-01-17", "2", 3);
    add("2011-01-01/2011-01-05", "2", 2);
    add("2011-01-03/2011-01-20", "1", 1);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-05", "2", 2),
            createExpected("2011-01-05/2011-01-13", "1", 1),
            createExpected("2011-01-13/2011-01-17", "2", 3),
            createExpected("2011-01-17/2011-01-20", "1", 1)
        ),
        timeline.lookup(Intervals.of("2011-01-01/2011-01-20"))
    );
  }

  //    1|-----------|
  //  2|----|     |----|
  @Test
  public void testOverlapV1FirstBetween()
  {
    add("2011-01-03/2011-01-17", "1", 1);
    add("2011-01-01/2011-01-05", "2", 2);
    add("2011-01-15/2011-01-20", "2", 3);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-05", "2", 2),
            createExpected("2011-01-05/2011-01-15", "1", 1),
            createExpected("2011-01-15/2011-01-20", "2", 3)
        ),
        timeline.lookup(Intervals.of("2011-01-01/2011-01-20"))
    );
  }

  //  2|----|     |----|
  //    1|-----------|
  @Test
  public void testOverlapV1SecondBetween()
  {
    add("2011-01-01/2011-01-05", "2", 2);
    add("2011-01-15/2011-01-20", "2", 3);
    add("2011-01-03/2011-01-17", "1", 1);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-05", "2", 2),
            createExpected("2011-01-05/2011-01-15", "1", 1),
            createExpected("2011-01-15/2011-01-20", "2", 3)
        ),
        timeline.lookup(Intervals.of("2011-01-01/2011-01-20"))
    );
  }

  //               4|---|
  //           3|---|
  //       2|---|
  // 1|-------------|
  @Test
  public void testOverlapLargeUnderlyingWithSmallDayAlignedOverlays()
  {
    add("2011-01-01/2011-01-05", "1", 1);
    add("2011-01-03/2011-01-04", "2", 2);
    add("2011-01-04/2011-01-05", "3", 3);
    add("2011-01-05/2011-01-06", "4", 4);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-03", "1", 1),
            createExpected("2011-01-03/2011-01-04", "2", 2),
            createExpected("2011-01-04/2011-01-05", "3", 3),
            createExpected("2011-01-05/2011-01-06", "4", 4)
        ),
        timeline.lookup(Intervals.of("0000-01-01/3000-01-01"))
    );
  }

  //     |----3---||---1---|
  //   |---2---|
  @Test
  public void testOverlapCausesNullEntries()
  {
    add("2011-01-01T12/2011-01-02", "3", 3);
    add("2011-01-02/3011-01-03", "1", 1);
    add("2011-01-01/2011-01-02", "2", 2);

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-01T12", "2", 2),
            createExpected("2011-01-01T12/2011-01-02", "3", 3),
            createExpected("2011-01-02/3011-01-03", "1", 1)
        ),
        timeline.lookup(Intervals.of("2011-01-01/3011-01-03"))
    );
  }

  //    1|----|    |----|
  //   2|------|  |------|
  //  3|------------------|
  @Test
  public void testOverlapOvershadowedThirdContains()
  {
    add("2011-01-03/2011-01-06", "1", 1);
    add("2011-01-09/2011-01-12", "1", 2);
    add("2011-01-02/2011-01-08", "2", 3);
    add("2011-01-10/2011-01-16", "2", 4);
    add("2011-01-01/2011-01-20", "3", 5);

    assertValues(
        Sets.newHashSet(
            createExpected("2011-01-02/2011-01-08", "2", 3),
            createExpected("2011-01-10/2011-01-16", "2", 4),
            createExpected("2011-01-03/2011-01-06", "1", 1),
            createExpected("2011-01-09/2011-01-12", "1", 2)
        ),
        timeline.findFullyOvershadowed()
    );
  }

  // 2|------|------|
  // 1|-------------|
  // 3|-------------|
  @Test
  public void testOverlapOvershadowedAligned()
  {
    add("2011-01-01/2011-01-05", "2", 1);
    add("2011-01-05/2011-01-10", "2", 2);
    add("2011-01-01/2011-01-10", "1", 3);
    add("2011-01-01/2011-01-10", "3", 4);

    assertValues(
        Sets.newHashSet(
            createExpected("2011-01-01/2011-01-05", "2", 1),
            createExpected("2011-01-05/2011-01-10", "2", 2),
            createExpected("2011-01-01/2011-01-10", "1", 3)
        ),
        timeline.findFullyOvershadowed()
    );
  }

  // 2|----|      |-----|
  //     1|---------|
  // 3|-----------|
  @Test
  public void testOverlapOvershadowedSomeComplexOverlapsCantThinkOfBetterName()
  {
    add("2011-01-01/2011-01-05", "2", 1);
    add("2011-01-10/2011-01-15", "2", 2);
    add("2011-01-03/2011-01-12", "1", 3);
    add("2011-01-01/2011-01-10", "3", 4);

    assertValues(
        Sets.newHashSet(
            createExpected("2011-01-03/2011-01-12", "1", 3),
            createExpected("2011-01-01/2011-01-05", "2", 1)
        ),
        timeline.findFullyOvershadowed()
    );
  }

  @Test
  public void testOverlapAndRemove()
  {
    add("2011-01-01/2011-01-20", "1", 1);
    add("2011-01-10/2011-01-15", "2", 2);

    timeline.remove(Intervals.of("2011-01-10/2011-01-15"), "2", makeSingle("2", 2));

    assertValues(
        Collections.singletonList(
            createExpected("2011-01-01/2011-01-20", "1", 1)
        ),
        timeline.lookup(Intervals.of("2011-01-01/2011-01-20"))
    );
  }

  @Test
  public void testOverlapAndRemove2()
  {
    add("2011-01-01/2011-01-20", "1", 1);
    add("2011-01-10/2011-01-20", "2", 2);
    add("2011-01-20/2011-01-30", "3", 4);

    timeline.remove(Intervals.of("2011-01-10/2011-01-20"), "2", makeSingle("2", 2));

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-20", "1", 1),
            createExpected("2011-01-20/2011-01-30", "3", 4)

        ),
        timeline.lookup(Intervals.of("2011-01-01/2011-01-30"))
    );
  }

  @Test
  public void testOverlapAndRemove3()
  {
    add("2011-01-01/2011-01-20", "1", 1);
    add("2011-01-02/2011-01-03", "2", 2);
    add("2011-01-10/2011-01-14", "2", 3);

    timeline.remove(Intervals.of("2011-01-02/2011-01-03"), "2", makeSingle("2", 2));
    timeline.remove(Intervals.of("2011-01-10/2011-01-14"), "2", makeSingle("2", 3));

    assertValues(
        Collections.singletonList(
            createExpected("2011-01-01/2011-01-20", "1", 1)

        ),
        timeline.lookup(Intervals.of("2011-01-01/2011-01-20"))
    );
  }

  @Test
  public void testOverlapAndRemove4()
  {
    add("2011-01-01/2011-01-20", "1", 1);
    add("2011-01-10/2011-01-15", "2", 2);
    add("2011-01-15/2011-01-20", "2", 3);

    timeline.remove(Intervals.of("2011-01-15/2011-01-20"), "2", makeSingle("2", 3));

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-10", "1", 1),
            createExpected("2011-01-10/2011-01-15", "2", 2),
            createExpected("2011-01-15/2011-01-20", "1", 1)
        ),
        timeline.lookup(Intervals.of("2011-01-01/2011-01-20"))
    );
  }

  @Test
  public void testOverlapAndRemove5()
  {
    add("2011-01-01/2011-01-20", "1", 1);
    add("2011-01-10/2011-01-15", "2", 2);
    timeline.remove(Intervals.of("2011-01-10/2011-01-15"), "2", makeSingle("2", 2));
    add("2011-01-01/2011-01-20", "1", 1);

    assertValues(
        Collections.singletonList(
            createExpected("2011-01-01/2011-01-20", "1", 1)
        ),
        timeline.lookup(Intervals.of("2011-01-01/2011-01-20"))
    );
  }

  @Test
  public void testRemoveNothingBacking()
  {
    add("2011-01-01/2011-01-05", "1", 1);
    add("2011-01-05/2011-01-10", "2", 2);
    add("2011-01-10/2011-01-15", "3", 3);
    add("2011-01-15/2011-01-20", "4", 4);

    timeline.remove(Intervals.of("2011-01-15/2011-01-20"), "4", makeSingle("4", 4));

    assertValues(
        Arrays.asList(
            createExpected("2011-01-01/2011-01-05", "1", 1),
            createExpected("2011-01-05/2011-01-10", "2", 2),
            createExpected("2011-01-10/2011-01-15", "3", 3)
        ),
        timeline.lookup(new Interval(DateTimes.EPOCH, DateTimes.MAX))
    );
  }

  @Test
  public void testOvershadowingHigherVersionWins1()
  {
    add("2011-04-01/2011-04-03", "1", 2);
    add("2011-04-03/2011-04-06", "1", 3);
    add("2011-04-06/2011-04-09", "1", 4);
    add("2011-04-01/2011-04-09", "2", 1);

    assertValues(
        ImmutableSet.of(
            createExpected("2011-04-01/2011-04-03", "1", 2),
            createExpected("2011-04-03/2011-04-06", "1", 3),
            createExpected("2011-04-06/2011-04-09", "1", 4)
        ),
        timeline.findFullyOvershadowed()
    );
  }

  @Test
  public void testOvershadowingHigherVersionWins2()
  {
    add("2011-04-01/2011-04-09", "1", 1);
    add("2011-04-01/2011-04-03", "2", 2);
    add("2011-04-03/2011-04-06", "2", 3);
    add("2011-04-06/2011-04-09", "2", 4);

    assertValues(
        ImmutableSet.of(
            createExpected("2011-04-01/2011-04-09", "1", 1)
        ),
        timeline.findFullyOvershadowed()
    );
  }

  @Test
  public void testOvershadowingHigherVersionWins3()
  {
    add("2011-04-01/2011-04-03", "1", 2);
    add("2011-04-03/2011-04-06", "1", 3);
    add("2011-04-09/2011-04-12", "1", 4);
    add("2011-04-01/2011-04-12", "2", 1);

    assertValues(
        Sets.newHashSet(
            createExpected("2011-04-01/2011-04-03", "1", 2),
            createExpected("2011-04-03/2011-04-06", "1", 3),
            createExpected("2011-04-09/2011-04-12", "1", 4)
        ),
        timeline.findFullyOvershadowed()
    );
  }

  @Test
  public void testOvershadowingHigherVersionWins4()
  {
    add("2011-04-03/2011-04-06", "1", 3);
    add("2011-04-06/2011-04-09", "1", 4);
    add("2011-04-01/2011-04-09", "2", 1);

    assertValues(
        ImmutableSet.of(
            createExpected("2011-04-03/2011-04-06", "1", 3),
            createExpected("2011-04-06/2011-04-09", "1", 4)
        ),
        timeline.findFullyOvershadowed()
    );
  }

  @Test
  public void testOvershadowingHigherVersionNeverOvershadowedByLower1()
  {
    add("2011-04-01/2011-04-09", "1", 1);
    add("2011-04-03/2011-04-06", "2", 3);
    add("2011-04-06/2011-04-09", "2", 4);

    assertValues(
        ImmutableSet.of(),
        timeline.findFullyOvershadowed()
    );
  }

  @Test
  public void testOvershadowingHigherVersionNeverOvershadowedByLower2()
  {
    add("2011-04-01/2011-04-09", "1", 1);
    add("2011-04-01/2011-04-03", "2", 2);
    add("2011-04-06/2011-04-09", "2", 4);

    assertValues(
        ImmutableSet.of(),
        timeline.findFullyOvershadowed()
    );
  }

  @Test
  public void testOvershadowingHigherVersionNeverOvershadowedByLower3()
  {
    add("2011-04-01/2011-04-09", "1", 1);
    add("2011-04-01/2011-04-03", "2", 2);
    add("2011-04-03/2011-04-06", "2", 3);

    assertValues(
        ImmutableSet.of(),
        timeline.findFullyOvershadowed()
    );
  }

  @Test
  public void testOvershadowingHigherVersionNeverOvershadowedByLower4()
  {
    add("2011-04-01/2011-04-09", "2", 1);
    add("2011-04-01/2011-04-03", "3", 2);
    add("2011-04-03/2011-04-06", "4", 3);
    add("2011-04-03/2011-04-06", "1", 3);

    assertValues(
        ImmutableSet.of(
            createExpected("2011-04-03/2011-04-06", "1", 3)
        ),
        timeline.findFullyOvershadowed()
    );
  }

  @Test
  public void testOvershadowingHigherVersionNeverOvershadowedByLower5()
  {
    add("2011-04-01/2011-04-12", "2", 1);
    add("2011-04-01/2011-04-03", "3", 2);
    add("2011-04-06/2011-04-09", "4", 3);
    add("2011-04-09/2011-04-12", "1", 3);
    add("2011-04-03/2011-04-06", "1", 3);

    assertValues(
        Sets.newHashSet(
            createExpected("2011-04-03/2011-04-06", "1", 3),
            createExpected("2011-04-09/2011-04-12", "1", 3)
        ),
        timeline.findFullyOvershadowed()
    );
  }

  @Test
  public void testOvershadowingSameIntervalHighVersionWins()
  {
    add("2011-04-01/2011-04-09", "1", 1);
    add("2011-04-01/2011-04-09", "9", 2);
    add("2011-04-01/2011-04-09", "2", 3);

    assertValues(
        Sets.newHashSet(
            createExpected("2011-04-01/2011-04-09", "2", 3),
            createExpected("2011-04-01/2011-04-09", "1", 1)
        ),
        timeline.findFullyOvershadowed()
    );
  }

  @Test
  public void testOvershadowingSameIntervalSameVersionAllKept()
  {
    add("2011-04-01/2011-04-09", "1", 1);
    add("2011-04-01/2011-04-09", "9", 2);
    add("2011-04-01/2011-04-09", "2", 3);
    add("2011-04-01/2011-04-09", "9", 4);

    assertValues(
        Sets.newHashSet(
            createExpected("2011-04-01/2011-04-09", "2", 3),
            createExpected("2011-04-01/2011-04-09", "1", 1)
        ),
        timeline.findFullyOvershadowed()
    );
  }

  @Test
  public void testNotFoundReturnsEmpty()
  {
    add("2011-04-01/2011-04-09", "1", 1);

    Assert.assertTrue(timeline.lookup(Intervals.of("1970/1980")).isEmpty());
  }

  /** https://github.com/apache/incubator-druid/issues/3010 */
  @Test
  public void testRemoveIncompleteKeepsComplete()
  {
    add("2011-04-01/2011-04-02", "1", IntegerPartitionChunk.make(null, 1, 0, new OvershadowableInteger("1", 0, 77)));
    add("2011-04-01/2011-04-02", "1", IntegerPartitionChunk.make(1, null, 1, new OvershadowableInteger("1", 1, 88)));
    add("2011-04-01/2011-04-02", "2", IntegerPartitionChunk.make(null, 1, 0, new OvershadowableInteger("2", 0, 99)));

    assertValues(
        ImmutableList.of(
            createExpected("2011-04-01/2011-04-02", "1",
                           Arrays.asList(
                               IntegerPartitionChunk.make(null, 1, 0, new OvershadowableInteger("1", 0, 77)),
                               IntegerPartitionChunk.make(1, null, 1, new OvershadowableInteger("1", 1, 88))
                           )
            )
        ),
        timeline.lookup(Intervals.of("2011-04-01/2011-04-02"))
    );

    add("2011-04-01/2011-04-02", "3", IntegerPartitionChunk.make(null, 1, 0, new OvershadowableInteger("3", 0, 110)));

    assertValues(
        ImmutableList.of(
            createExpected("2011-04-01/2011-04-02", "1",
                           Arrays.asList(
                               IntegerPartitionChunk.make(null, 1, 0, new OvershadowableInteger("1", 0, 77)),
                               IntegerPartitionChunk.make(1, null, 1, new OvershadowableInteger("1", 1, 88))
                           )
            )
        ),
        timeline.lookup(Intervals.of("2011-04-01/2011-04-02"))
    );
    assertValues(
        Sets.newHashSet(
            createExpected("2011-04-01/2011-04-02", "2",
                Collections.singletonList(
                    IntegerPartitionChunk.make(null, 1, 0, new OvershadowableInteger("2", 0, 99))
                )
            )
        ),
        timeline.findFullyOvershadowed()
    );

    checkRemove();

    assertValues(
        ImmutableList.of(
            createExpected("2011-04-01/2011-04-02", "1",
                           Arrays.asList(
                               IntegerPartitionChunk.make(null, 1, 0, new OvershadowableInteger("1", 0, 77)),
                               IntegerPartitionChunk.make(1, null, 1, new OvershadowableInteger("1", 1, 88))
                           )
            )
        ),
        timeline.lookup(Intervals.of("2011-04-01/2011-04-02"))
    );
  }

  @Test
  public void testIsOvershadowedWithNonOverlappingSegmentsInTimeline()
  {
    add("2011-04-05/2011-04-07", "1", makeSingle("1", 1));
    add("2011-04-07/2011-04-09", "1", makeSingle("1", 1));

    add("2011-04-15/2011-04-17", "1", makeSingle("1", 1));
    add("2011-04-17/2011-04-19", "1", makeSingle("1", 1));

    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-01/2011-04-03"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-01/2011-04-05"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-01/2011-04-06"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-01/2011-04-07"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-01/2011-04-08"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-01/2011-04-09"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-01/2011-04-10"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-01/2011-04-30"), "0", new OvershadowableInteger("0", 0, 1)));

    Assert.assertTrue(timeline.isOvershadowed(Intervals.of("2011-04-05/2011-04-06"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertTrue(timeline.isOvershadowed(Intervals.of("2011-04-05/2011-04-07"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertTrue(timeline.isOvershadowed(Intervals.of("2011-04-05/2011-04-08"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertTrue(timeline.isOvershadowed(Intervals.of("2011-04-05/2011-04-09"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-05/2011-04-06"), "1", new OvershadowableInteger("1", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-05/2011-04-07"), "1", new OvershadowableInteger("1", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-05/2011-04-08"), "1", new OvershadowableInteger("1", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-05/2011-04-09"), "1", new OvershadowableInteger("1", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-05/2011-04-06"), "2", new OvershadowableInteger("2", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-05/2011-04-07"), "2", new OvershadowableInteger("2", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-05/2011-04-08"), "2", new OvershadowableInteger("2", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-05/2011-04-09"), "2", new OvershadowableInteger("2", 0, 1)));

    Assert.assertTrue(timeline.isOvershadowed(Intervals.of("2011-04-06/2011-04-07"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertTrue(timeline.isOvershadowed(Intervals.of("2011-04-06/2011-04-08"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertTrue(timeline.isOvershadowed(Intervals.of("2011-04-06/2011-04-09"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-06/2011-04-10"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-06/2011-04-30"), "0", new OvershadowableInteger("0", 0, 1)));

    Assert.assertTrue(timeline.isOvershadowed(Intervals.of("2011-04-07/2011-04-08"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertTrue(timeline.isOvershadowed(Intervals.of("2011-04-07/2011-04-09"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-07/2011-04-10"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-07/2011-04-30"), "0", new OvershadowableInteger("0", 0, 1)));

    Assert.assertTrue(timeline.isOvershadowed(Intervals.of("2011-04-08/2011-04-09"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-08/2011-04-10"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-08/2011-04-30"), "0", new OvershadowableInteger("0", 0, 1)));

    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-09/2011-04-10"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-09/2011-04-15"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-09/2011-04-17"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-09/2011-04-19"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-09/2011-04-30"), "0", new OvershadowableInteger("0", 0, 1)));

    Assert.assertTrue(timeline.isOvershadowed(Intervals.of("2011-04-15/2011-04-16"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertTrue(timeline.isOvershadowed(Intervals.of("2011-04-15/2011-04-17"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertTrue(timeline.isOvershadowed(Intervals.of("2011-04-15/2011-04-18"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertTrue(timeline.isOvershadowed(Intervals.of("2011-04-15/2011-04-19"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-15/2011-04-20"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-15/2011-04-30"), "0", new OvershadowableInteger("0", 0, 1)));

    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-19/2011-04-20"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-21/2011-04-22"), "0", new OvershadowableInteger("0", 0, 1)));
  }

  @Test
  public void testIsOvershadowedWithOverlappingSegmentsInTimeline()
  {
    add("2011-04-05/2011-04-09", "11", makeSingle("11", 1));
    add("2011-04-07/2011-04-11", "12", makeSingle("12", 1));

    add("2011-04-15/2011-04-19", "12", makeSingle("12", 1));
    add("2011-04-17/2011-04-21", "11", makeSingle("11", 1));

    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-01/2011-04-03"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-01/2011-04-05"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-01/2011-04-06"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-01/2011-04-07"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-01/2011-04-08"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-01/2011-04-09"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-01/2011-04-10"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-01/2011-04-11"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-01/2011-04-30"), "0", new OvershadowableInteger("0", 0, 1)));

    Assert.assertTrue(timeline.isOvershadowed(Intervals.of("2011-04-05/2011-04-06"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertTrue(timeline.isOvershadowed(Intervals.of("2011-04-05/2011-04-07"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertTrue(timeline.isOvershadowed(Intervals.of("2011-04-05/2011-04-08"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertTrue(timeline.isOvershadowed(Intervals.of("2011-04-05/2011-04-09"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertTrue(timeline.isOvershadowed(Intervals.of("2011-04-05/2011-04-10"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertTrue(timeline.isOvershadowed(Intervals.of("2011-04-05/2011-04-11"), "0", new OvershadowableInteger("0", 0, 1)));

    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-05/2011-04-06"), "12", new OvershadowableInteger("12", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-05/2011-04-07"), "12", new OvershadowableInteger("12", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-05/2011-04-08"), "12", new OvershadowableInteger("12", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-05/2011-04-09"), "12", new OvershadowableInteger("12", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-05/2011-04-10"), "12", new OvershadowableInteger("12", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-05/2011-04-11"), "12", new OvershadowableInteger("12", 0, 1)));

    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-05/2011-04-06"), "13", new OvershadowableInteger("13", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-05/2011-04-07"), "13", new OvershadowableInteger("13", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-05/2011-04-08"), "13", new OvershadowableInteger("13", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-05/2011-04-09"), "13", new OvershadowableInteger("13", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-05/2011-04-10"), "13", new OvershadowableInteger("13", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-05/2011-04-11"), "13", new OvershadowableInteger("13", 0, 1)));

    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-05/2011-04-12"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-05/2011-04-15"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-05/2011-04-16"), "0", new OvershadowableInteger("0", 0, 1)));

    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-05/2011-04-17"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-05/2011-04-18"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-05/2011-04-19"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-05/2011-04-20"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-05/2011-04-21"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-05/2011-04-22"), "0", new OvershadowableInteger("0", 0, 1)));

    Assert.assertTrue(timeline.isOvershadowed(Intervals.of("2011-04-06/2011-04-07"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertTrue(timeline.isOvershadowed(Intervals.of("2011-04-06/2011-04-08"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertTrue(timeline.isOvershadowed(Intervals.of("2011-04-06/2011-04-09"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertTrue(timeline.isOvershadowed(Intervals.of("2011-04-06/2011-04-10"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertTrue(timeline.isOvershadowed(Intervals.of("2011-04-06/2011-04-11"), "0", new OvershadowableInteger("0", 0, 1)));

    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-06/2011-04-12"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-06/2011-04-15"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-06/2011-04-16"), "0", new OvershadowableInteger("0", 0, 1)));

    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-06/2011-04-17"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-06/2011-04-18"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-06/2011-04-19"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-06/2011-04-20"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-06/2011-04-21"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-06/2011-04-22"), "0", new OvershadowableInteger("0", 0, 1)));

    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-12/2011-04-15"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-12/2011-04-16"), "0", new OvershadowableInteger("0", 0, 1)));

    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-12/2011-04-17"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-12/2011-04-18"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-12/2011-04-19"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-12/2011-04-20"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-12/2011-04-21"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-12/2011-04-22"), "0", new OvershadowableInteger("0", 0, 1)));

    Assert.assertTrue(timeline.isOvershadowed(Intervals.of("2011-04-15/2011-04-21"), "0", new OvershadowableInteger("0", 0, 1)));
    Assert.assertFalse(timeline.isOvershadowed(Intervals.of("2011-04-21/2011-04-22"), "0", new OvershadowableInteger("0", 0, 1)));
  }

  @Test
  public void testOvershadowedByReference()
  {
    add("2019-01-01/2019-01-02", "0", makeNumbered("0", 0, 0));
    add("2019-01-01/2019-01-02", "0", makeNumbered("0", 1, 0));
    add("2019-01-01/2019-01-02", "0", makeNumbered("0", 2, 0));

    add("2019-01-01/2019-01-02", "0", makeNumberedOverwriting("0", 0, 1, 0, 3, 1, 2));
    add("2019-01-01/2019-01-02", "0", makeNumberedOverwriting("0", 1, 1, 0, 3, 1, 2));

    Assert.assertEquals(
        ImmutableSet.of(
            makeTimelineObjectHolder(
                "2019-01-01/2019-01-02",
                "0",
                ImmutableList.of(makeNumbered("0", 0, 0), makeNumbered("0", 1, 0), makeNumbered("0", 2, 0))
            )
        ),
        timeline.findFullyOvershadowed()
    );
  }

  @Test
  public void testOvershadowedByReferenceChain()
  {
    // 2019-01-01/2019-01-02
    add("2019-01-01/2019-01-02", "0", makeNumbered("0", 0, 0));
    add("2019-01-01/2019-01-02", "0", makeNumbered("0", 1, 0));
    add("2019-01-01/2019-01-02", "0", makeNumbered("0", 2, 0));

    // 2019-01-02/2019-01-03
    add("2019-01-02/2019-01-03", "0", makeNumbered("0", 0, 0));
    add("2019-01-02/2019-01-03", "0", makeNumbered("0", 1, 0));

    // Overwrite 2019-01-01/2019-01-02
    add("2019-01-01/2019-01-02", "0", makeNumberedOverwriting("0", 0, 1, 0, 3, 1, 2));
    add("2019-01-01/2019-01-02", "0", makeNumberedOverwriting("0", 1, 1, 0, 3, 1, 2));

    // Overwrite 2019-01-01/2019-01-02
    add("2019-01-01/2019-01-02", "0", makeNumberedOverwriting("0", 2, 2, 0, 3, 2, 2));
    add("2019-01-01/2019-01-02", "0", makeNumberedOverwriting("0", 3, 2, 0, 3, 2, 2));

    Assert.assertEquals(
        ImmutableSet.of(
            makeTimelineObjectHolder(
                "2019-01-01/2019-01-02",
                "0",
                ImmutableList.of(
                    makeNumbered("0", 0, 0),
                    makeNumbered("0", 1, 0),
                    makeNumbered("0", 2, 0),
                    makeNumberedOverwriting("0", 0, 1, 0, 3, 1, 2),
                    makeNumberedOverwriting("0", 1, 1, 0, 3, 1, 2)
                )
            )
        ),
        timeline.findFullyOvershadowed()
    );
  }

  @Test
  public void testOvershadowedByReferenceAndThenVersion()
  {
    // 2019-01-01/2019-01-02
    add("2019-01-01/2019-01-02", "0", makeNumbered("0", 0, 0));
    add("2019-01-01/2019-01-02", "0", makeNumbered("0", 1, 0));
    add("2019-01-01/2019-01-02", "0", makeNumbered("0", 2, 0));

    // 2019-01-02/2019-01-03
    add("2019-01-02/2019-01-03", "0", makeNumbered("0", 0, 0));
    add("2019-01-02/2019-01-03", "0", makeNumbered("0", 1, 0));

    // Overwrite 2019-01-01/2019-01-02
    add("2019-01-01/2019-01-02", "0", makeNumberedOverwriting("0", 0, 1, 0, 3, 1, 2));
    add("2019-01-01/2019-01-02", "0", makeNumberedOverwriting("0", 1, 1, 0, 3, 1, 2));

    // Overwrite 2019-01-01/2019-01-02
    add("2019-01-01/2019-01-02", "1", makeNumbered("1", 0, 0));
    add("2019-01-01/2019-01-02", "1", makeNumbered("1", 1, 0));

    Assert.assertEquals(
        ImmutableSet.of(
            makeTimelineObjectHolder(
                "2019-01-01/2019-01-02",
                "0",
                ImmutableList.of(
                    makeNumbered("0", 0, 0),
                    makeNumbered("0", 1, 0),
                    makeNumbered("0", 2, 0),
                    makeNumberedOverwriting("0", 0, 1, 0, 3, 1, 2),
                    makeNumberedOverwriting("0", 1, 1, 0, 3, 1, 2)
                )
            )
        ),
        timeline.findFullyOvershadowed()
    );
  }

  @Test
  public void testOvershadowedByVersionAndThenReference()
  {
    // 2019-01-01/2019-01-02
    add("2019-01-01/2019-01-02", "0", makeNumbered("0", 0, 0));
    add("2019-01-01/2019-01-02", "0", makeNumbered("0", 1, 0));
    add("2019-01-01/2019-01-02", "0", makeNumbered("0", 2, 0));

    // 2019-01-02/2019-01-03
    add("2019-01-02/2019-01-03", "0", makeNumbered("0", 0, 0));
    add("2019-01-02/2019-01-03", "0", makeNumbered("0", 1, 0));

    // Overwrite 2019-01-01/2019-01-02
    add("2019-01-01/2019-01-02", "1", makeNumbered("1", 0, 0));
    add("2019-01-01/2019-01-02", "1", makeNumbered("1", 1, 0));

    // Overwrite 2019-01-01/2019-01-02
    add("2019-01-01/2019-01-02", "1", makeNumberedOverwriting("1", 0, 1, 0, 2, 1, 3));
    add("2019-01-01/2019-01-02", "1", makeNumberedOverwriting("1", 1, 1, 0, 2, 1, 3));
    add("2019-01-01/2019-01-02", "1", makeNumberedOverwriting("1", 2, 1, 0, 2, 1, 3));

    Assert.assertEquals(
        ImmutableSet.of(
            makeTimelineObjectHolder(
                "2019-01-01/2019-01-02",
                "0",
                ImmutableList.of(
                    makeNumbered("0", 0, 0),
                    makeNumbered("0", 1, 0),
                    makeNumbered("0", 2, 0)
                )
            ),
            makeTimelineObjectHolder(
                "2019-01-01/2019-01-02",
                "1",
                ImmutableList.of(
                    makeNumbered("1", 0, 0),
                    makeNumbered("1", 1, 0)
                )
            )
        ),
        timeline.findFullyOvershadowed()
    );
  }

  @Test
  public void testFallbackOnMissingSegment()
  {
    final Interval interval = Intervals.of("2019-01-01/2019-01-02");

    add(interval, "0", makeNumbered("0", 0, 0));
    add(interval, "0", makeNumbered("0", 1, 0));
    add(interval, "0", makeNumbered("0", 2, 0));

    // Overwrite 2019-01-01/2019-01-02
    add(interval, "1", makeNumbered("1", 0, 0));
    add(interval, "1", makeNumbered("1", 1, 0));

    // Overwrite 2019-01-01/2019-01-02
    add("2019-01-01/2019-01-02", "1", makeNumberedOverwriting("1", 0, 1, 0, 2, 1, 3));
    add("2019-01-01/2019-01-02", "1", makeNumberedOverwriting("1", 1, 1, 0, 2, 1, 3));
    add("2019-01-01/2019-01-02", "1", makeNumberedOverwriting("1", 2, 1, 0, 2, 1, 3));

    timeline.remove(
        interval,
        "1",
        makeNumberedOverwriting("1", 2, 1, 0, 2, 1, 3)
    );

    final List<TimelineObjectHolder<String, OvershadowableInteger>> holders = timeline.lookup(interval);

    Assert.assertEquals(
        ImmutableList.of(
            new TimelineObjectHolder<>(
                interval,
                "1",
                new PartitionHolder<>(
                    ImmutableList.of(
                        makeNumbered("1", 0, 0),
                        makeNumbered("1", 1, 0),
                        makeNumberedOverwriting("1", 0, 1, 0, 2, 1, 3),
                        makeNumberedOverwriting("1", 1, 1, 0, 2, 1, 3)
                    )
                )
            )
        ),
        holders
    );
  }

  @Test
  public void testAddSameChunkToFullAtomicUpdateGroup()
  {
    final Interval interval = Intervals.of("2019-01-01/2019-01-02");
    add(interval, "0", makeNumbered("0", 0, 0));
    add(interval, "0", makeNumberedOverwriting("0", 0, 0, 0, 1, 1, 1));
    add(interval, "0", makeNumbered("0", 0, 1));

    final Set<TimelineObjectHolder<String, OvershadowableInteger>> overshadowed = timeline.findFullyOvershadowed();
    Assert.assertEquals(
        ImmutableSet.of(
            new TimelineObjectHolder<>(
                interval,
                "0",
                new PartitionHolder<>(ImmutableList.of(makeNumbered("0", 0, 1)))
            )
        ),
        overshadowed
    );
  }

  @Test
  public void testOvershadowMultipleStandbyAtomicUpdateGroup()
  {
    final Interval interval = Intervals.of("2019-01-01/2019-01-02");
    add(interval, "0", makeNumberedOverwriting("0", 0, 0, 0, 1, 1, 2));
    add(interval, "0", makeNumberedOverwriting("0", 1, 0, 0, 1, 2, 2));
    add(interval, "0", makeNumberedOverwriting("0", 2, 0, 0, 1, 3, 2)); // <-- full atomicUpdateGroup
    add(interval, "0", makeNumberedOverwriting("0", 3, 1, 0, 1, 3, 2)); // <-- full atomicUpdateGroup

    final Set<TimelineObjectHolder<String, OvershadowableInteger>> overshadowed = timeline.findFullyOvershadowed();
    Assert.assertEquals(
        ImmutableSet.of(
            new TimelineObjectHolder<>(
                interval,
                "0",
                new PartitionHolder<>(
                    ImmutableList.of(
                        makeNumberedOverwriting("0", 0, 0, 0, 1, 1, 2),
                        makeNumberedOverwriting("0", 1, 0, 0, 1, 2, 2)
                    )
                )
            )
        ),
        overshadowed
    );
  }

  @Test
  public void testIsOvershadowedForOverwritingSegments()
  {
    final Interval interval = Intervals.of("2019-01-01/2019-01-02");
    add(interval, "0", makeNumberedOverwriting("0", 0, 0, 5, 10, 10, 1));

    for (int i = 0; i < 5; i++) {
      Assert.assertTrue(timeline.isOvershadowed(interval, "0", makeNumbered("0", i + 5, 0).getObject()));
    }

    Assert.assertFalse(timeline.isOvershadowed(interval, "0", makeNumbered("0", 4, 0).getObject()));
    Assert.assertFalse(timeline.isOvershadowed(interval, "0", makeNumbered("0", 11, 0).getObject()));

    Assert.assertTrue(timeline.isOvershadowed(interval, "0", makeNumberedOverwriting("0", 1, 0, 5, 6, 5, 2).getObject()));
    Assert.assertTrue(timeline.isOvershadowed(interval, "0", makeNumberedOverwriting("0", 1, 0, 7, 8, 5, 2).getObject()));
    Assert.assertTrue(timeline.isOvershadowed(interval, "0", makeNumberedOverwriting("0", 1, 0, 8, 10, 5, 2).getObject()));

    Assert.assertFalse(timeline.isOvershadowed(interval, "0", makeNumberedOverwriting("0", 1, 0, 5, 10, 12, 2).getObject()));
    Assert.assertFalse(timeline.isOvershadowed(interval, "0", makeNumberedOverwriting("0", 1, 0, 4, 15, 12, 2).getObject()));
  }

  @Test
  public void testIterateAllObjects() {
    add("2011-01-01/2011-01-10", "1", 1);
    add("2011-01-01/2011-01-10", "2", 3);

    Collection<OvershadowableInteger> overshadowableIntegers = timeline.iterateAllObjects();
    Assert.assertEquals(2, overshadowableIntegers.size());
    // Tests that the "iteration" size of the returned collection is 2 (the "reported" size from size() method may be
    // deceptive).
    Assert.assertEquals(2, Lists.newArrayList(overshadowableIntegers.iterator()).size());
  }

  void checkRemove()
  {
    for (TimelineObjectHolder<String, OvershadowableInteger> holder : timeline.findFullyOvershadowed()) {
      for (PartitionChunk<OvershadowableInteger> chunk : holder.getObject()) {
        timeline.remove(holder.getInterval(), holder.getVersion(), chunk);
      }
    }

    Assert.assertTrue(timeline.findFullyOvershadowed().isEmpty());
  }

  private TimelineObjectHolder<String, OvershadowableInteger> makeTimelineObjectHolder(
      String interval,
      String version,
      List<PartitionChunk<OvershadowableInteger>> chunks
  )
  {
    return new TimelineObjectHolder<>(
        Intervals.of(interval),
        Intervals.of(interval),
        version,
        new PartitionHolder<>(chunks)
    );
  }

  Pair<Interval, Pair<String, PartitionHolder<OvershadowableInteger>>> createExpected(
      String intervalString,
      String version,
      Integer value
  )
  {
    return createExpected(
        intervalString,
        version,
        Collections.singletonList(makeSingle(version, value))
    );
  }

  Pair<Interval, Pair<String, PartitionHolder<OvershadowableInteger>>> createExpected(
      String intervalString,
      String version,
      List<PartitionChunk<OvershadowableInteger>> values
  )
  {
    return Pair.of(
        Intervals.of(intervalString),
        Pair.of(version, new PartitionHolder<>(values))
    );
  }

  PartitionChunk<OvershadowableInteger> makeSingle(String majorVersion, int value)
  {
    return makeSingle(majorVersion, 0, value);
  }

  private PartitionChunk<OvershadowableInteger> makeSingle(String majorVersion, int partitionNum, int val)
  {
    return new SingleElementPartitionChunk<>(new OvershadowableInteger(majorVersion, partitionNum, val));
  }

  private PartitionChunk<OvershadowableInteger> makeNumbered(String majorVersion, int partitionNum, int val)
  {
    return new NumberedPartitionChunk<>(
        partitionNum,
        0,
        new OvershadowableInteger(majorVersion, partitionNum, val)
    );
  }

  private PartitionChunk<OvershadowableInteger> makeNumberedOverwriting(
      String majorVersion,
      int partitionNumOrdinal,
      int val,
      int startRootPartitionId,
      int endRootPartitionId,
      int minorVersion,
      int atomicUpdateGroupSize
  )
  {
    final int partitionNum = PartitionIds.NON_ROOT_GEN_START_PARTITION_ID + partitionNumOrdinal;
    return new NumberedOverwritingPartitionChunk<>(
        partitionNum,
        new OvershadowableInteger(
            majorVersion,
            partitionNum,
            val,
            startRootPartitionId,
            endRootPartitionId,
            minorVersion,
            atomicUpdateGroupSize
        )
    );
  }

  void add(String interval, String version, Integer value)
  {
    add(Intervals.of(interval), version, value);
  }

  void add(Interval interval, String version, Integer value)
  {
    add(interval, version, makeSingle(version, value));
  }

  void add(String interval, String version, PartitionChunk<OvershadowableInteger> value)
  {
    add(Intervals.of(interval), version, value);
  }

  private void add(Interval interval, String version, PartitionChunk<OvershadowableInteger> value)
  {
    timeline.add(interval, version, value);
  }

  void assertValues(
      List<Pair<Interval, Pair<String, PartitionHolder<OvershadowableInteger>>>> expected,
      List<TimelineObjectHolder<String, OvershadowableInteger>> actual
  )
  {
    Assert.assertEquals("Sizes did not match.", expected.size(), actual.size());

    Iterator<Pair<Interval, Pair<String, PartitionHolder<OvershadowableInteger>>>> expectedIter = expected.iterator();
    Iterator<TimelineObjectHolder<String, OvershadowableInteger>> actualIter = actual.iterator();

    while (expectedIter.hasNext()) {
      Pair<Interval, Pair<String, PartitionHolder<OvershadowableInteger>>> pair = expectedIter.next();
      TimelineObjectHolder<String, OvershadowableInteger> holder = actualIter.next();

      Assert.assertEquals(pair.lhs, holder.getInterval());
      Assert.assertEquals(pair.rhs.lhs, holder.getVersion());

      final List<PartitionChunk<OvershadowableInteger>> expectedChunks = Lists.newArrayList(pair.rhs.rhs);
      final List<PartitionChunk<OvershadowableInteger>> actualChunks = Lists.newArrayList(holder.getObject());

      Assert.assertEquals(expectedChunks.size(), actualChunks.size());
      for (int i = 0; i < expectedChunks.size(); i++) {
        // Check partitionNumber first
        Assert.assertEquals(expectedChunks.get(i), actualChunks.get(i));
        final OvershadowableInteger expectedInteger = expectedChunks.get(i).getObject();
        final OvershadowableInteger actualInteger = actualChunks.get(i).getObject();
        Assert.assertEquals(expectedInteger, actualInteger);
      }
    }
  }

  void assertValues(
      Set<Pair<Interval, Pair<String, PartitionHolder<OvershadowableInteger>>>> expected,
      Set<TimelineObjectHolder<String, OvershadowableInteger>> actual
  )
  {
    Assert.assertEquals("Sizes did not match.", expected.size(), actual.size());

    Set<Pair<Interval, Pair<String, PartitionHolder<OvershadowableInteger>>>> actualSet =
        Sets.newHashSet(
            Iterables.transform(
                actual,
                input -> new Pair<>(input.getInterval(), new Pair<>(input.getVersion(), input.getObject()))
            )
        );

    Assert.assertEquals(expected, actualSet);
  }

  VersionedIntervalTimeline<String, OvershadowableInteger> makeStringIntegerTimeline()
  {
    return new VersionedIntervalTimeline<>(Ordering.natural());
  }
}
