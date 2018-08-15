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

package io.druid.indexing.overlord;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import io.druid.indexer.TaskLocation;
import io.druid.indexer.TaskStatus;
import io.druid.indexing.worker.Worker;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.emitter.EmittingLogger;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.concurrent.Executor;

public class TaskRunnerUtils
{
  private static final EmittingLogger log = new EmittingLogger(TaskRunnerUtils.class);

  public static void notifyLocationChanged(
      final Iterable<Pair<TaskRunnerListener, Executor>> listeners,
      final String taskId,
      final TaskLocation location
  )
  {
    log.info("Task [%s] location changed to [%s].", taskId, location);
    for (final Pair<TaskRunnerListener, Executor> listener : listeners) {
      try {
        listener.rhs.execute(
            new Runnable()
            {
              @Override
              public void run()
              {
                listener.lhs.locationChanged(taskId, location);
              }
            }
        );
      }
      catch (Exception e) {
        log.makeAlert(e, "Unable to notify task listener")
           .addData("taskId", taskId)
           .addData("taskLocation", location)
           .addData("listener", listener.toString())
           .emit();
      }
    }
  }

  public static void notifyStatusChanged(
      final Iterable<Pair<TaskRunnerListener, Executor>> listeners,
      final String taskId,
      final TaskStatus status
  )
  {
    log.info("Task [%s] status changed to [%s].", taskId, status.getStatusCode());
    for (final Pair<TaskRunnerListener, Executor> listener : listeners) {
      try {
        listener.rhs.execute(
            new Runnable()
            {
              @Override
              public void run()
              {
                listener.lhs.statusChanged(taskId, status);
              }
            }
        );
      }
      catch (Exception e) {
        log.makeAlert(e, "Unable to notify task listener")
           .addData("taskId", taskId)
           .addData("taskStatus", status.getStatusCode())
           .addData("listener", listener.toString())
           .emit();
      }
    }
  }

  public static URL makeWorkerURL(Worker worker, String pathFormat, Object... pathParams)
  {
    Preconditions.checkArgument(pathFormat.startsWith("/"), "path must start with '/': %s", pathFormat);
    final String path = StringUtils.format(
        pathFormat,
        Arrays.stream(pathParams).map(s -> StringUtils.urlEncode(s.toString())).toArray()
    );

    try {
      return new URI(worker.getScheme(), worker.getHost(), path, null, null).toURL();
    }
    catch (URISyntaxException | MalformedURLException e) {
      throw Throwables.propagate(e);
    }
  }
}
