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

package org.apache.druid.query.rollingavgquery.averagers;

public class LongMeanAverager extends BaseAverager<Number, Double>
{

  private int startFrom = 0;

  public LongMeanAverager(int numBuckets, String name, String fieldName, int period)
  {
    super(Number.class, numBuckets, name, fieldName, period);
  }

  @Override
  protected Double computeResult()
  {
    long result = 0;
    int validBuckets = 0;
    int period = getPeriod();
    int numBuckets = getNumBuckets();
    Number[] obj = getBuckets();

    for (int i = 0; i < numBuckets; i += period) {
      if (obj[(i + startFrom) % numBuckets] != null) {
        result += (obj[(i + startFrom) % numBuckets]).longValue();
      } else {
        result += 0;
      }
      validBuckets++;
    }

    startFrom++;
    return ((double) result) / validBuckets;
  }
}
