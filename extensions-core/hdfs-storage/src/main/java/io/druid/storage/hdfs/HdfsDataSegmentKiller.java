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

package io.druid.storage.hdfs;

import com.google.inject.Inject;
import com.metamx.emitter.EmittingLogger;
import io.druid.segment.loading.DataSegmentKiller;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class HdfsDataSegmentKiller implements DataSegmentKiller
{
  private static final EmittingLogger log = new EmittingLogger(HdfsDataSegmentKiller.class);

  private static final String PATH_KEY = "path";

  private final Configuration config;

  private final Path storageDirectory;

  @Inject
  public HdfsDataSegmentKiller(final Configuration config, final HdfsDataSegmentPusherConfig pusherConfig)
  {
    this.config = config;
    this.storageDirectory = new Path(pusherConfig.getStorageDirectory());
  }

  @Override
  public void kill(DataSegment segment) throws SegmentLoadingException
  {
    final Path path = getPath(segment);
    log.info("killing segment[%s] mapped to path[%s]", segment.getIdentifier(), path);

    try {
      String segmentLocation = path.getName();
      final FileSystem fs = path.getFileSystem(config);
      if (!segmentLocation.endsWith(".zip")) {
        throw new SegmentLoadingException("Unknown file type[%s]", path);
      } else {

        if (!fs.exists(path)) {
          log.warn("Segment Path [%s] does not exist. It appears to have been deleted already.", path);
          return;
        }

        String[] zipParts = segmentLocation.split("_");
        // for segments stored as hdfs://nn1/hdfs_base_directory/data_source_name/interval/version/shardNum_index.zip
        if (zipParts.length == 2
            && zipParts[1].equals("index.zip")
            && StringUtils.isNumeric(zipParts[0])) {
          if (!fs.delete(path, false)) {
            throw new SegmentLoadingException(
                "Unable to kill segment, failed to delete [%s]",
                path.toString()
            );
          }
          Path descriptorPath = new Path(path.getParent(), String.format("%s_descriptor.json", zipParts[0]));
          //delete partitionNumber_descriptor.json
          if (!fs.delete(descriptorPath, false)) {
            throw new SegmentLoadingException(
                "Unable to kill segment, failed to delete [%s]",
                descriptorPath.toString()
            );
          }
        } else { //for segments stored as hdfs://nn1/hdfs_base_directory/data_source_name/interval/version/shardNum/index.zip
          if (!fs.delete(path, false)) {
            throw new SegmentLoadingException(
                "Unable to kill segment, failed to delete [%s]",
                path.toString()
            );
          }
          Path descriptorPath = new Path(path.getParent(), "descriptor.json");
          if (!fs.delete(descriptorPath, false)) {
            throw new SegmentLoadingException(
                "Unable to kill segment, failed to delete [%s]",
                descriptorPath.toString()
            );
          }

        }

        deleteEmptyParents(fs, path.getParent());
      }
    }
    catch (IOException e) {
      throw new SegmentLoadingException(e, "Unable to kill segment");
    }
  }

  @Override
  public void killAll() throws IOException
  {
    log.info("Deleting all segment files from hdfs dir [%s].", storageDirectory.toUri().toString());
    final FileSystem fs = storageDirectory.getFileSystem(config);
    fs.delete(storageDirectory, true);
  }

  private void deleteEmptyParents(FileSystem fs, Path path)
  {
    if(fs.makeQualified(path).equals(fs.makeQualified(storageDirectory))) {
      return;
    }
    try {
      if(fs.listStatus(path).length == 0 && fs.delete(path, false))
      {
        deleteEmptyParents(fs, path.getParent());
      }
    }
    catch (Exception e) {
      log.makeAlert(e, "uncaught exception during segment killer").emit();
    }
  }

  private Path getPath(DataSegment segment)
  {
    return new Path(String.valueOf(segment.getLoadSpec().get(PATH_KEY)));
  }
}
