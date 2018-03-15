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

package io.druid.indexing.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import io.druid.indexer.TaskReport;
import io.druid.indexer.TaskState;

import javax.annotation.Nullable;

/**
 * Represents the status of a task from the perspective of the coordinator. The task may be ongoing
 * ({@link #isComplete()} false) or it may be complete ({@link #isComplete()} true).
 *
 * TaskStatus objects are immutable.
 */
public class TaskStatus
{
  public static TaskStatus running(String taskId)
  {
    return new TaskStatus(taskId, TaskState.RUNNING, null, -1);
  }

  public static TaskStatus success(String taskId)
  {
    return new TaskStatus(taskId, TaskState.SUCCESS, null, -1);
  }

  public static TaskStatus success(String taskId, TaskReport report)
  {
    return new TaskStatus(taskId, TaskState.SUCCESS, report, -1);
  }

  public static TaskStatus failure(String taskId)
  {
    return new TaskStatus(taskId, TaskState.FAILED, null, -1);
  }

  public static TaskStatus fromCode(String taskId, TaskState code)
  {
    return new TaskStatus(taskId, code, null, -1);
  }

  private final String id;
  private final TaskState status;
  private final TaskReport report;
  private final long duration;

  @JsonCreator
  protected TaskStatus(
      @JsonProperty("id") String id,
      @JsonProperty("status") TaskState status,
      @JsonProperty("report") @Nullable TaskReport report,
      @JsonProperty("duration") long duration
  )
  {
    this.id = id;
    this.status = status;
    this.report = report;
    this.duration = duration;

    // Check class invariants.
    Preconditions.checkNotNull(id, "id");
    Preconditions.checkNotNull(status, "status");
  }

  @JsonProperty("id")
  public String getId()
  {
    return id;
  }

  @JsonProperty("status")
  public TaskState getStatusCode()
  {
    return status;
  }

  @JsonProperty("report")
  public TaskReport getReport()
  {
    return report;
  }

  @JsonProperty("duration")
  public long getDuration()
  {
    return duration;
  }

  /**
   * Signals that a task is not yet complete, and is still runnable on a worker. Exactly one of isRunnable,
   * isSuccess, or isFailure will be true at any one time.
   *
   * @return whether the task is runnable.
   */
  @JsonIgnore
  public boolean isRunnable()
  {
    return status == TaskState.RUNNING;
  }

  /**
   * Inverse of {@link #isRunnable}.
   *
   * @return whether the task is complete.
   */
  @JsonIgnore
  public boolean isComplete()
  {
    return !isRunnable();
  }

  /**
   * Returned by tasks when they spawn subtasks. Exactly one of isRunnable, isSuccess, or isFailure will
   * be true at any one time.
   *
   * @return whether the task succeeded.
   */
  @JsonIgnore
  public boolean isSuccess()
  {
    return status == TaskState.SUCCESS;
  }

  /**
   * Returned by tasks when they complete unsuccessfully. Exactly one of isRunnable, isSuccess, or
   * isFailure will be true at any one time.
   *
   * @return whether the task failed
   */
  @JsonIgnore
  public boolean isFailure()
  {
    return status == TaskState.FAILED;
  }

  public TaskStatus withDuration(long _duration)
  {
    return new TaskStatus(id, status, report, _duration);
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
    TaskStatus that = (TaskStatus) o;
    return duration == that.duration &&
           java.util.Objects.equals(id, that.id) &&
           status == that.status &&
           java.util.Objects.equals(report, that.report);
  }

  @Override
  public int hashCode()
  {
    return java.util.Objects.hash(id, status, report, duration);
  }

  @Override
  public String toString()
  {
    return Objects.toStringHelper(this)
                  .add("id", id)
                  .add("status", status)
                  .add("report", report)
                  .add("duration", duration)
                  .toString();
  }
}
