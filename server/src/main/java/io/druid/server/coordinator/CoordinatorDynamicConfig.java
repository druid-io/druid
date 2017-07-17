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
package io.druid.server.coordinator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import io.druid.java.util.common.IAE;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class CoordinatorDynamicConfig
{
  public static final String CONFIG_KEY = "coordinator.config";

  private final long millisToWaitBeforeDeleting;
  private final long mergeBytesLimit;
  private final int mergeSegmentsLimit;
  private final int maxSegmentsToMove;
  private final int replicantLifetime;
  private final int replicationThrottleLimit;
  private final int balancerComputeThreads;
  private final boolean emitBalancingStats;
  private final boolean killAllDataSources;
  private final Set<String> killDataSourceWhitelist;
  /**
   * The maximum number of segments that could be queued for loading to any given server.
   * Default values is 0 with the meaning of "unbounded" (any number of
   * segments could be added to the loading queue for any server).
   * See {@link LoadQueuePeon}, {@link io.druid.server.coordinator.rules.LoadRule#run}
   */
  private final int maxSegmentsInNodeLoadingQueue;

  @JsonCreator
  public CoordinatorDynamicConfig(
      @JsonProperty("millisToWaitBeforeDeleting") long millisToWaitBeforeDeleting,
      @JsonProperty("mergeBytesLimit") long mergeBytesLimit,
      @JsonProperty("mergeSegmentsLimit") int mergeSegmentsLimit,
      @JsonProperty("maxSegmentsToMove") int maxSegmentsToMove,
      @JsonProperty("replicantLifetime") int replicantLifetime,
      @JsonProperty("replicationThrottleLimit") int replicationThrottleLimit,
      @JsonProperty("balancerComputeThreads") int balancerComputeThreads,
      @JsonProperty("emitBalancingStats") boolean emitBalancingStats,

      // Type is Object here so that we can support both string and list as
      // coordinator console can not send array of strings in the update request.
      // See https://github.com/druid-io/druid/issues/3055
      @JsonProperty("killDataSourceWhitelist") Object killDataSourceWhitelist,
      @JsonProperty("killAllDataSources") boolean killAllDataSources,
      @JsonProperty("maxSegmentsInNodeLoadingQueue") int maxSegmentsInNodeLoadingQueue
  )
  {
    this.millisToWaitBeforeDeleting = millisToWaitBeforeDeleting;
    this.mergeBytesLimit = mergeBytesLimit;
    this.mergeSegmentsLimit = mergeSegmentsLimit;
    this.maxSegmentsToMove = maxSegmentsToMove;
    this.replicantLifetime = replicantLifetime;
    this.replicationThrottleLimit = replicationThrottleLimit;
    this.balancerComputeThreads = Math.max(balancerComputeThreads, 1);
    this.emitBalancingStats = emitBalancingStats;
    this.killAllDataSources = killAllDataSources;
    this.killDataSourceWhitelist = parseKillDataSourceWhitelist(killDataSourceWhitelist);
    this.maxSegmentsInNodeLoadingQueue = maxSegmentsInNodeLoadingQueue;

    if (this.killAllDataSources && !this.killDataSourceWhitelist.isEmpty()) {
      throw new IAE("can't have killAllDataSources and non-empty killDataSourceWhitelist");
    }
  }

  private Set<String> parseKillDataSourceWhitelist(Object killDataSourceWhitelist)
  {
    if (killDataSourceWhitelist instanceof String) {
      String[] tmp = ((String) killDataSourceWhitelist).split(",");
      Set<String> result = new HashSet<>();
      for (int i = 0; i < tmp.length; i++) {
        String trimmed = tmp[i].trim();
        if (!trimmed.isEmpty()) {
          result.add(trimmed);
        }
      }
      return result;
    } else if (killDataSourceWhitelist instanceof Collection) {
      return ImmutableSet.copyOf(((Collection) killDataSourceWhitelist));
    } else {
      return ImmutableSet.of();
    }
  }

  @JsonProperty
  public long getMillisToWaitBeforeDeleting()
  {
    return millisToWaitBeforeDeleting;
  }

  @JsonProperty
  public long getMergeBytesLimit()
  {
    return mergeBytesLimit;
  }

  @JsonProperty
  public boolean emitBalancingStats()
  {
    return emitBalancingStats;
  }

  @JsonProperty
  public int getMergeSegmentsLimit()
  {
    return mergeSegmentsLimit;
  }

  @JsonProperty
  public int getMaxSegmentsToMove()
  {
    return maxSegmentsToMove;
  }

  @JsonProperty
  public int getReplicantLifetime()
  {
    return replicantLifetime;
  }

  @JsonProperty
  public int getReplicationThrottleLimit()
  {
    return replicationThrottleLimit;
  }

  @JsonProperty
  public int getBalancerComputeThreads()
  {
    return balancerComputeThreads;
  }

  @JsonProperty
  public Set<String> getKillDataSourceWhitelist()
  {
    return killDataSourceWhitelist;
  }

  @JsonProperty
  public boolean isKillAllDataSources()
  {
    return killAllDataSources;
  }

  @JsonProperty
  public int getMaxSegmentsInNodeLoadingQueue()
  {
    return maxSegmentsInNodeLoadingQueue;
  }

  @Override
  public String toString()
  {
    return "CoordinatorDynamicConfig{" +
           "millisToWaitBeforeDeleting=" + millisToWaitBeforeDeleting +
           ", mergeBytesLimit=" + mergeBytesLimit +
           ", mergeSegmentsLimit=" + mergeSegmentsLimit +
           ", maxSegmentsToMove=" + maxSegmentsToMove +
           ", replicantLifetime=" + replicantLifetime +
           ", replicationThrottleLimit=" + replicationThrottleLimit +
           ", balancerComputeThreads=" + balancerComputeThreads +
           ", emitBalancingStats=" + emitBalancingStats +
           ", killDataSourceWhitelist=" + killDataSourceWhitelist +
           ", killAllDataSources=" + killAllDataSources +
           ", maxSegmentsInNodeLoadingQueue=" + maxSegmentsInNodeLoadingQueue +
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

    CoordinatorDynamicConfig that = (CoordinatorDynamicConfig) o;

    if (millisToWaitBeforeDeleting != that.millisToWaitBeforeDeleting) {
      return false;
    }
    if (mergeBytesLimit != that.mergeBytesLimit) {
      return false;
    }
    if (mergeSegmentsLimit != that.mergeSegmentsLimit) {
      return false;
    }
    if (maxSegmentsToMove != that.maxSegmentsToMove) {
      return false;
    }
    if (replicantLifetime != that.replicantLifetime) {
      return false;
    }
    if (replicationThrottleLimit != that.replicationThrottleLimit) {
      return false;
    }
    if (balancerComputeThreads != that.balancerComputeThreads) {
      return false;
    }
    if (emitBalancingStats != that.emitBalancingStats) {
      return false;
    }
    if (killAllDataSources != that.killAllDataSources) {
      return false;
    }
    if (maxSegmentsInNodeLoadingQueue != that.maxSegmentsInNodeLoadingQueue) {
      return false;
    }
    return !(killDataSourceWhitelist != null
             ? !killDataSourceWhitelist.equals(that.killDataSourceWhitelist)
             : that.killDataSourceWhitelist != null);

  }

  @Override
  public int hashCode()
  {
    int result = (int) (millisToWaitBeforeDeleting ^ (millisToWaitBeforeDeleting >>> 32));
    result = 31 * result + (int) (mergeBytesLimit ^ (mergeBytesLimit >>> 32));
    result = 31 * result + mergeSegmentsLimit;
    result = 31 * result + maxSegmentsToMove;
    result = 31 * result + replicantLifetime;
    result = 31 * result + replicationThrottleLimit;
    result = 31 * result + balancerComputeThreads;
    result = 31 * result + (emitBalancingStats ? 1 : 0);
    result = 31 * result + (killAllDataSources ? 1 : 0);
    result = 31 * result + (killDataSourceWhitelist != null ? killDataSourceWhitelist.hashCode() : 0);
    result = 31 * result + maxSegmentsInNodeLoadingQueue;
    return result;
  }

  public static Builder builder()
  {
    return new Builder();
  }

  public static class Builder
  {
    private Long millisToWaitBeforeDeleting;
    private Long mergeBytesLimit;
    private Integer mergeSegmentsLimit;
    private Integer maxSegmentsToMove;
    private Integer replicantLifetime;
    private Integer replicationThrottleLimit;
    private Boolean emitBalancingStats;
    private Integer balancerComputeThreads;
    private Object killDataSourceWhitelist;
    private Boolean killAllDataSources;
    private Integer maxSegmentsInNodeLoadingQueue;

    public Builder()
    {
    }

    @JsonCreator
    public Builder(
        @JsonProperty("millisToWaitBeforeDeleting") Long millisToWaitBeforeDeleting,
        @JsonProperty("mergeBytesLimit") Long mergeBytesLimit,
        @JsonProperty("mergeSegmentsLimit") Integer mergeSegmentsLimit,
        @JsonProperty("maxSegmentsToMove") Integer maxSegmentsToMove,
        @JsonProperty("replicantLifetime") Integer replicantLifetime,
        @JsonProperty("replicationThrottleLimit") Integer replicationThrottleLimit,
        @JsonProperty("balancerComputeThreads") Integer balancerComputeThreads,
        @JsonProperty("emitBalancingStats") Boolean emitBalancingStats,
        @JsonProperty("killDataSourceWhitelist") Object killDataSourceWhitelist,
        @JsonProperty("killAllDataSources") Boolean killAllDataSources,
        @JsonProperty("maxSegmentsInNodeLoadingQueue") Integer maxSegmentsInNodeLoadingQueue
    )
    {
      this.millisToWaitBeforeDeleting = millisToWaitBeforeDeleting;
      this.mergeBytesLimit = mergeBytesLimit;
      this.mergeSegmentsLimit = mergeSegmentsLimit;
      this.maxSegmentsToMove = maxSegmentsToMove;
      this.replicantLifetime = replicantLifetime;
      this.replicationThrottleLimit = replicationThrottleLimit;
      this.balancerComputeThreads = balancerComputeThreads;
      this.emitBalancingStats = emitBalancingStats;
      this.killAllDataSources = killAllDataSources;
      this.killDataSourceWhitelist = killDataSourceWhitelist;
      this.maxSegmentsInNodeLoadingQueue = maxSegmentsInNodeLoadingQueue;
    }

    public Builder withMillisToWaitBeforeDeleting(long millisToWaitBeforeDeleting)
    {
      this.millisToWaitBeforeDeleting = millisToWaitBeforeDeleting;
      return this;
    }

    public Builder withMergeBytesLimit(long mergeBytesLimit)
    {
      this.mergeBytesLimit = mergeBytesLimit;
      return this;
    }

    public Builder withMergeSegmentsLimit(int mergeSegmentsLimit)
    {
      this.mergeSegmentsLimit = mergeSegmentsLimit;
      return this;
    }

    public Builder withMaxSegmentsToMove(int maxSegmentsToMove)
    {
      this.maxSegmentsToMove = maxSegmentsToMove;
      return this;
    }

    public Builder withReplicantLifetime(int replicantLifetime)
    {
      this.replicantLifetime = replicantLifetime;
      return this;
    }

    public Builder withReplicationThrottleLimit(int replicationThrottleLimit)
    {
      this.replicationThrottleLimit = replicationThrottleLimit;
      return this;
    }

    public Builder withBalancerComputeThreads(int balancerComputeThreads)
    {
      this.balancerComputeThreads = balancerComputeThreads;
      return this;
    }

    public Builder withEmitBalancingStats(boolean emitBalancingStats)
    {
      this.emitBalancingStats = emitBalancingStats;
      return this;
    }

    public Builder withKillDataSourceWhitelist(Set<String> killDataSourceWhitelist)
    {
      this.killDataSourceWhitelist = killDataSourceWhitelist;
      return this;
    }

    public Builder withKillAllDataSources(boolean killAllDataSources)
    {
      this.killAllDataSources = killAllDataSources;
      return this;
    }

    public Builder withMaxSegmentsInNodeLoadingQueue(int maxSegmentsInNodeLoadingQueue)
    {
      this.maxSegmentsInNodeLoadingQueue = maxSegmentsInNodeLoadingQueue;
      return this;
    }

    public CoordinatorDynamicConfig build()
    {
      return new CoordinatorDynamicConfig(
          millisToWaitBeforeDeleting == null ? 15 * 60 * 1000L : millisToWaitBeforeDeleting,
          mergeBytesLimit == null ? 524288000L : mergeBytesLimit,
          mergeSegmentsLimit == null ? 100 : mergeSegmentsLimit,
          maxSegmentsToMove == null ? 5 : maxSegmentsToMove,
          replicantLifetime == null ? 15 : replicantLifetime,
          replicationThrottleLimit == null ? 10 : replicationThrottleLimit,
          balancerComputeThreads == null ? 1 : balancerComputeThreads,
          emitBalancingStats == null ? false : emitBalancingStats,
          killDataSourceWhitelist,
          killAllDataSources == null ? false : killAllDataSources,
          maxSegmentsInNodeLoadingQueue == null ? 0 : maxSegmentsInNodeLoadingQueue
      );
    }

    public CoordinatorDynamicConfig build(CoordinatorDynamicConfig defaults)
    {
      return new CoordinatorDynamicConfig(
          millisToWaitBeforeDeleting == null ? defaults.getMillisToWaitBeforeDeleting() : millisToWaitBeforeDeleting,
          mergeBytesLimit == null ? defaults.getMergeBytesLimit() : mergeBytesLimit,
          mergeSegmentsLimit == null ? defaults.getMergeSegmentsLimit() : mergeSegmentsLimit,
          maxSegmentsToMove == null ? defaults.getMaxSegmentsToMove() : maxSegmentsToMove,
          replicantLifetime == null ? defaults.getReplicantLifetime() : replicantLifetime,
          replicationThrottleLimit == null ? defaults.getReplicationThrottleLimit() : replicationThrottleLimit,
          balancerComputeThreads == null ? defaults.getBalancerComputeThreads() : balancerComputeThreads,
          emitBalancingStats == null ? defaults.emitBalancingStats() : emitBalancingStats,
          killDataSourceWhitelist == null ? defaults.getKillDataSourceWhitelist() : killDataSourceWhitelist,
          killAllDataSources == null ? defaults.isKillAllDataSources() : killAllDataSources,
          maxSegmentsInNodeLoadingQueue == null ? defaults.getMaxSegmentsInNodeLoadingQueue() : maxSegmentsInNodeLoadingQueue
      );
    }
  }
}
