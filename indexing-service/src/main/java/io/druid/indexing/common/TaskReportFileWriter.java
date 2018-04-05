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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.java.util.common.logger.Logger;
import org.apache.commons.io.FileUtils;

import java.io.File;

public class TaskReportFileWriter
{
  private static final Logger log = new Logger(TaskReportFileWriter.class);

  private final File reportsFile;
  private ObjectMapper objectMapper;

  public TaskReportFileWriter(File reportFile)
  {
    this.reportsFile = reportFile;
  }

  public void write(TaskReport report)
  {
    try {
      final File reportsFileParent = reportsFile.getParentFile();
      if (reportsFileParent != null) {
        FileUtils.forceMkdir(reportsFileParent);
      }
      objectMapper.writeValue(reportsFile, report);
    }
    catch (Exception e) {
      log.error(e, "Encountered exception in write().");
    }
  }

  public void setObjectMapper(ObjectMapper objectMapper)
  {
    this.objectMapper = objectMapper;
  }
}
