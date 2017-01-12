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

package io.druid.server.lookup.cache;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.Duration;

import javax.validation.constraints.Min;

public class LookupCoordinatorManagerConfig
{
  public static final Duration DEFAULT_HOST_TIMEOUT = Duration.millis(10_000L);
  public static final Duration DEFAULT_ALL_HOST_TIMEOUT = Duration.millis(300_000L);

  @JsonProperty
  private Duration hostTimeout = null;
  @JsonProperty
  private Duration allHostTimeout = null;
  @JsonProperty
  @Min(2)
  private int threadPoolSize = 10;
  @JsonProperty
  @Min(1)
  private long period = 120_000L;

  public Duration getHostTimeout()
  {
    return hostTimeout == null ? DEFAULT_HOST_TIMEOUT : hostTimeout;
  }

  public void setHostTimeout(Duration hostTimeout)
  {
    this.hostTimeout = hostTimeout;
  }

  public Duration getAllHostTimeout()
  {
    return allHostTimeout == null ? DEFAULT_ALL_HOST_TIMEOUT : allHostTimeout;
  }

  public void setAllHostTimeout(Duration allHostTimeout)
  {
    this.allHostTimeout = allHostTimeout;
  }

  public int getThreadPoolSize()
  {
    return threadPoolSize;
  }

  public void setThreadPoolSize(int threadPoolSize)
  {
    this.threadPoolSize = threadPoolSize;
  }

  public long getPeriod()
  {
    return period;
  }

  public void setPeriod(long period)
  {
    this.period = period;
  }
}
