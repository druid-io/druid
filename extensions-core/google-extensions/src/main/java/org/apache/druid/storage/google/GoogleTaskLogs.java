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

package org.apache.druid.storage.google;

import com.google.api.client.http.InputStreamContent;
import com.google.common.base.Optional;
import com.google.common.io.ByteSource;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.tasklogs.TaskLogs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class GoogleTaskLogs implements TaskLogs
{
  private static final Logger LOG = new Logger(GoogleTaskLogs.class);

  private final GoogleTaskLogsConfig config;
  private final GoogleStorage storage;

  @Inject
  public GoogleTaskLogs(GoogleTaskLogsConfig config, GoogleStorage storage)
  {
    this.config = config;
    this.storage = storage;
  }

  @Override
  public void pushTaskLog(final String taskid, final File logFile) throws IOException
  {
    final String taskKey = getTaskLogKey(taskid);
    LOG.info("Pushing task log %s to: %s", logFile, taskKey);
    pushTaskFile(logFile, taskKey);
  }

  @Override
  public void pushTaskReports(String taskid, File reportFile) throws IOException
  {
    final String taskKey = getTaskReportKey(taskid);
    LOG.info("Pushing task reports %s to: %s", reportFile, taskKey);
    pushTaskFile(reportFile, taskKey);
  }

  private void pushTaskFile(final File logFile, final String taskKey) throws IOException
  {
    try (final FileInputStream fileStream = new FileInputStream(logFile)) {

      InputStreamContent mediaContent = new InputStreamContent("text/plain", fileStream);
      mediaContent.setLength(logFile.length());

      try {
        RetryUtils.retry(
            (RetryUtils.Task<Void>) () -> {
              storage.insert(config.getBucket(), taskKey, mediaContent);
              return null;
            },
            GoogleUtils::isRetryable,
            1,
            5
        );
      }
      catch (IOException e) {
        throw e;
      }
      catch (Exception e) {
        throw new RE(e, "Failed to upload [%s] to [%s]", logFile, taskKey);
      }
    }
  }

  @Override
  public Optional<ByteSource> streamTaskLog(final String taskid, final long offset) throws IOException
  {
    final String taskKey = getTaskLogKey(taskid);
    return streamTaskFile(taskid, offset, taskKey);
  }

  @Override
  public Optional<ByteSource> streamTaskReports(String taskid) throws IOException
  {
    final String taskKey = getTaskReportKey(taskid);
    return streamTaskFile(taskid, 0, taskKey);
  }

  private Optional<ByteSource> streamTaskFile(final String taskid, final long offset, String taskKey) throws IOException
  {
    try {
      if (!storage.exists(config.getBucket(), taskKey)) {
        return Optional.absent();
      }

      final long length = storage.size(config.getBucket(), taskKey);

      return Optional.of(
          new ByteSource()
          {
            @Override
            public InputStream openStream() throws IOException
            {
              try {
                final long start;

                if (offset > 0 && offset < length) {
                  start = offset;
                } else if (offset < 0 && (-1 * offset) < length) {
                  start = length + offset;
                } else {
                  start = 0;
                }

                InputStream stream = new GoogleByteSource(storage, config.getBucket(), taskKey).openStream();
                stream.skip(start);

                return stream;
              }
              catch (Exception e) {
                throw new IOException(e);
              }
            }
          }
      );
    }
    catch (IOException e) {
      throw new IOE(e, "Failed to stream logs from: %s", taskKey);
    }
  }

  private String getTaskLogKey(String taskid)
  {
    return config.getPrefix() + "/" + taskid.replace(':', '_');
  }

  private String getTaskReportKey(String taskid)
  {
    return config.getPrefix() + "/" + taskid.replace(':', '_') + ".report.json";
  }

  @Override
  public void killAll()
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void killOlderThan(long timestamp)
  {
    throw new UnsupportedOperationException("not implemented");
  }
}
