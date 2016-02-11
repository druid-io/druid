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

package io.druid.segment.filter;

import io.druid.segment.Rowboat;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class RowboatTest
{
  @Test
  public void testRowboatCompare()
  {
    Rowboat rb1 = new Rowboat(12345L, new Comparable[][]{new Comparable[]{1}, new Comparable[]{2}}, new Object[]{new Integer(7)}, 5);
    Rowboat rb2 = new Rowboat(12345L, new Comparable[][]{new Comparable[]{1}, new Comparable[]{2}}, new Object[]{new Integer(7)}, 5);
    Assert.assertEquals(0, rb1.compareTo(rb2));

    Rowboat rb3 = new Rowboat(12345L, new Comparable[][]{new Comparable[]{3}, new Comparable[]{2}}, new Object[]{new Integer(7)}, 5);
    Assert.assertNotEquals(0, rb1.compareTo(rb3));
  }

  @Test
  public void testBiggerCompare()
  {
    Rowboat rb1 = new Rowboat(
        0,
        new Comparable[][]{
            new Comparable[]{0},
            new Comparable[]{138},
            new Comparable[]{44},
            new Comparable[]{374},
            new Comparable[]{0},
            new Comparable[]{0},
            new Comparable[]{552},
            new Comparable[]{338},
            new Comparable[]{910},
            new Comparable[]{25570},
            new Comparable[]{9},
            new Comparable[]{0},
            new Comparable[]{0},
            new Comparable[]{0}
        },
        new Object[]{1.0, 47.0, "someMetric"},
        0
    );

    Rowboat rb2 = new Rowboat(
        0,
        new Comparable[][]{
            new Comparable[]{0},
            new Comparable[]{138},
            new Comparable[]{44},
            new Comparable[]{374},
            new Comparable[]{0},
            new Comparable[]{0},
            new Comparable[]{553},
            new Comparable[]{338},
            new Comparable[]{910},
            new Comparable[]{25580},
            new Comparable[]{9},
            new Comparable[]{0},
            new Comparable[]{0},
            new Comparable[]{0}
        },
        new Object[]{1.0, 47.0, "someMetric"},
        0
    );

    Assert.assertNotEquals(0, rb1.compareTo(rb2));
  }

  @Test
  public void testToString()
  {
    Assert.assertEquals(
        "Rowboat{timestamp=1970-01-01T00:00:00.000Z, dims=[[1], [2]], metrics=[someMetric], comprisedRows={}}",
        new Rowboat(0, new Comparable[][]{new Comparable[]{1}, new Comparable[]{2}}, new Object[]{"someMetric"}, 5).toString()
    );
  }

  @Test
  public void testLotsONullString()
  {
    Assert.assertEquals(
        "Rowboat{timestamp=1970-01-01T00:00:00.000Z, dims=null, metrics=null, comprisedRows={}}",
        new Rowboat(0, null, null, 5).toString()
    );
  }
}
