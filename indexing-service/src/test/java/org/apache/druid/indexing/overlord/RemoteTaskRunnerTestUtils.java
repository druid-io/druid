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

package org.apache.druid.indexing.overlord;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingCluster;
import org.apache.druid.common.guava.DSuppliers;
import org.apache.druid.curator.PotentiallyGzippedCompressionProvider;
import org.apache.druid.curator.cache.PathChildrenCacheFactory;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.IndexingServiceCondition;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.autoscaling.NoopProvisioningStrategy;
import org.apache.druid.indexing.overlord.autoscaling.ProvisioningStrategy;
import org.apache.druid.indexing.overlord.config.RemoteTaskRunnerConfig;
import org.apache.druid.indexing.overlord.setup.DefaultWorkerBehaviorConfig;
import org.apache.druid.indexing.overlord.setup.WorkerBehaviorConfig;
import org.apache.druid.indexing.worker.TaskAnnouncement;
import org.apache.druid.indexing.worker.Worker;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.server.initialization.IndexerZkConfig;
import org.apache.druid.server.initialization.ZkPathsConfig;
import org.apache.zookeeper.CreateMode;
import org.easymock.EasyMock;

import java.util.concurrent.atomic.AtomicReference;

/**
 */
public class RemoteTaskRunnerTestUtils
{
  static final Joiner joiner = Joiner.on("/");
  static final String basePath = "/test/druid";
  static final String announcementsPath = StringUtils.format("%s/indexer/announcements", basePath);
  static final String tasksPath = StringUtils.format("%s/indexer/tasks", basePath);
  static final String statusPath = StringUtils.format("%s/indexer/status", basePath);
  static final TaskLocation DUMMY_LOCATION = TaskLocation.create("dummy", 9000, -1);

  private TestingCluster testingCluster;

  private CuratorFramework cf;
  private ObjectMapper jsonMapper;
  private HttpClient httpClient;

  RemoteTaskRunnerTestUtils()
  {
    TestUtils testUtils = new TestUtils();
    jsonMapper = testUtils.getTestObjectMapper();
  }

  CuratorFramework getCuratorFramework()
  {
    return cf;
  }

  ObjectMapper getObjectMapper()
  {
    return jsonMapper;
  }

  HttpClient getHttpClient()
  {
    return httpClient;
  }

  void setUp() throws Exception
  {
    testingCluster = new TestingCluster(1);
    testingCluster.start();

    cf = CuratorFrameworkFactory.builder()
                                .connectString(testingCluster.getConnectString())
                                .retryPolicy(new ExponentialBackoffRetry(1, 10))
                                .compressionProvider(new PotentiallyGzippedCompressionProvider(false))
                                .build();
    cf.start();
    cf.blockUntilConnected();
    cf.create().creatingParentsIfNeeded().forPath(basePath);
    cf.create().creatingParentsIfNeeded().forPath(tasksPath);

    httpClient = EasyMock.createMock(HttpClient.class);
  }

  void tearDown() throws Exception
  {
    cf.close();
    testingCluster.stop();
  }

  RemoteTaskRunner makeRemoteTaskRunner(RemoteTaskRunnerConfig config)
  {
    NoopProvisioningStrategy<WorkerTaskRunner> resourceManagement = new NoopProvisioningStrategy<>();
    return makeRemoteTaskRunner(config, resourceManagement);
  }

  public RemoteTaskRunner makeRemoteTaskRunner(
      RemoteTaskRunnerConfig config,
      ProvisioningStrategy<WorkerTaskRunner> provisioningStrategy
  )
  {
    RemoteTaskRunner remoteTaskRunner = new TestableRemoteTaskRunner(
        jsonMapper,
        config,
        new IndexerZkConfig(
            new ZkPathsConfig()
            {
              @Override
              public String getBase()
              {
                return basePath;
              }
            }, null, null, null, null
        ),
        cf,
        new PathChildrenCacheFactory.Builder(),
        httpClient,
        DSuppliers.of(new AtomicReference<>(DefaultWorkerBehaviorConfig.defaultConfig())),
        provisioningStrategy
    );

    remoteTaskRunner.start();
    return remoteTaskRunner;
  }

  Worker makeWorker(final String workerId, final int capacity) throws Exception
  {
    Worker worker = new Worker(
        "http",
        workerId,
        workerId,
        capacity,
        "0"
    );

    cf.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(
        joiner.join(announcementsPath, workerId),
        jsonMapper.writeValueAsBytes(worker)
    );
    cf.create().creatingParentsIfNeeded().forPath(joiner.join(tasksPath, workerId));

    return worker;
  }

  void disableWorker(Worker worker) throws Exception
  {
    cf.setData().forPath(
        joiner.join(announcementsPath, worker.getHost()),
        jsonMapper.writeValueAsBytes(new Worker(worker.getScheme(), worker.getHost(), worker.getIp(), worker.getCapacity(), ""))
    );
  }

  void mockWorkerRunningTask(final String workerId, final Task task) throws Exception
  {
    cf.delete().forPath(joiner.join(tasksPath, workerId, task.getId()));

    final String taskStatusPath = joiner.join(statusPath, workerId, task.getId());
    TaskAnnouncement taskAnnouncement = TaskAnnouncement.create(task, TaskStatus.running(task.getId()), DUMMY_LOCATION);
    cf.create()
      .creatingParentsIfNeeded()
      .forPath(taskStatusPath, jsonMapper.writeValueAsBytes(taskAnnouncement));

    Preconditions.checkNotNull(
        cf.checkExists().forPath(taskStatusPath),
        "Failed to write status on [%s]",
        taskStatusPath
    );
  }

  void mockWorkerCompleteSuccessfulTask(final String workerId, final Task task) throws Exception
  {
    TaskAnnouncement taskAnnouncement = TaskAnnouncement.create(task, TaskStatus.success(task.getId()), DUMMY_LOCATION);
    cf.setData().forPath(joiner.join(statusPath, workerId, task.getId()), jsonMapper.writeValueAsBytes(taskAnnouncement));
  }

  void mockWorkerCompleteFailedTask(final String workerId, final Task task) throws Exception
  {
    TaskAnnouncement taskAnnouncement = TaskAnnouncement.create(task, TaskStatus.failure(task.getId()), DUMMY_LOCATION);
    cf.setData().forPath(joiner.join(statusPath, workerId, task.getId()), jsonMapper.writeValueAsBytes(taskAnnouncement));
  }

  boolean workerRunningTask(final String workerId, final String taskId)
  {
    return pathExists(joiner.join(statusPath, workerId, taskId));
  }

  boolean taskAnnounced(final String workerId, final String taskId)
  {
    return pathExists(joiner.join(tasksPath, workerId, taskId));
  }

  boolean pathExists(final String path)
  {
    return TestUtils.conditionValid(
        new IndexingServiceCondition()
        {
          @Override
          public boolean isValid()
          {
            try {
              return cf.checkExists().forPath(path) != null;
            }
            catch (Exception e) {
              throw Throwables.propagate(e);
            }
          }

          @Override
          public String toString()
          {
            return StringUtils.format("Path[%s] exists", path);
          }
        }
    );
  }

  public static class TestableRemoteTaskRunner extends RemoteTaskRunner
  {
    private long currentTimeMillis = System.currentTimeMillis();

    public TestableRemoteTaskRunner(
        ObjectMapper jsonMapper,
        RemoteTaskRunnerConfig config,
        IndexerZkConfig indexerZkConfig,
        CuratorFramework cf,
        PathChildrenCacheFactory.Builder pathChildrenCacheFactory,
        HttpClient httpClient,
        Supplier<WorkerBehaviorConfig> workerConfigRef,
        ProvisioningStrategy<WorkerTaskRunner> provisioningStrategy
    )
    {
      super(
          jsonMapper,
          config,
          indexerZkConfig,
          cf,
          pathChildrenCacheFactory,
          httpClient,
          workerConfigRef,
          provisioningStrategy
      );
    }

    void setCurrentTimeMillis(long currentTimeMillis)
    {
      this.currentTimeMillis = currentTimeMillis;
    }

    @Override
    protected long getCurrentTimeMillis()
    {
      return currentTimeMillis;
    }
  }
}
