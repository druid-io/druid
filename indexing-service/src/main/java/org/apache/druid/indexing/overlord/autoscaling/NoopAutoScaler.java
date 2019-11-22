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

package org.apache.druid.indexing.overlord.autoscaling;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.emitter.EmittingLogger;

import java.util.List;

/**
 * This class just logs when scaling should occur.
 */
public class NoopAutoScaler<Void> implements AutoScaler<Void>
{
  private static final EmittingLogger log = new EmittingLogger(NoopAutoScaler.class);
  private final String category;

  @JsonCreator
  public NoopAutoScaler(
      @JsonProperty("category") String category
  )
  {
    this.category = category;
  }

  @Override
  public int getMinNumWorkers()
  {
    return 0;
  }

  @Override
  public int getMaxNumWorkers()
  {
    return 0;
  }

  @Override
  @JsonProperty
  public String getCategory()
  {
    return category;
  }

  @Override
  public Void getEnvConfig()
  {
    throw new UOE("No config for Noop!");
  }

  @Override
  public AutoScalingData provision()
  {
    log.info("If I were a real strategy I'd create something now [category: %s]", category);
    return null;
  }

  @Override
  public AutoScalingData terminate(List<String> ips)
  {
    log.info("If I were a real strategy I'd terminate %s now [category: %s]", ips, category);
    return null;
  }

  @Override
  public AutoScalingData terminateWithIds(List<String> ids)
  {
    log.info("If I were a real strategy I'd terminate %s now [category: %s]", ids, category);
    return null;
  }

  @Override
  public List<String> ipToIdLookup(List<String> ips)
  {
    log.info("I'm not a real strategy so I'm returning what I got %s [category: %s]", ips, category);
    return ips;
  }

  @Override
  public List<String> idToIpLookup(List<String> nodeIds)
  {
    log.info("I'm not a real strategy so I'm returning what I got %s [category: %s]", nodeIds, category);
    return nodeIds;
  }

  @Override
  public String toString()
  {
    return "NoopAutoScaler{" +
           "category='" + category + '\'' +
           '}';
  }
}
