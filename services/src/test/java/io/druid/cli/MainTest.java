/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.cli;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.Injector;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.NodeTypeConfig;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Map;

@RunWith(Parameterized.class)
public class MainTest
{
  private Map<Class, String> nodeTypeMap = ImmutableMap.<Class, String>builder()
      .put(CliOverlord.class, "overlord")
      .put(CliBroker.class, "broker")
      .put(CliHistorical.class, "historical")
      .put(CliCoordinator.class, "coordinator")
      .put(CliMiddleManager.class, "middleManager")
      .put(CliRealtime.class, "realtime")
      .put(CliRealtimeExample.class, "realtime")
      .put(CliRouter.class, "router").build();

  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[]{new CliOverlord()},
        new Object[]{new CliBroker()},

        // Takes arguments. Cannot be used in this test
        //new Object[]{new CliPeon()},

        new Object[]{new CliHistorical()},
        new Object[]{new CliCoordinator()},

        // Implements Runnable, not GuiceRunnable
        //new Object[]{new CliHadoopIndexer()},

        // Takes arguments. Cannot be used in this test
        //new Object[]{new CliInternalHadoopIndexer()},

        new Object[]{new CliMiddleManager()},
        new Object[]{new CliRealtime()},
        new Object[]{new CliRealtimeExample()},
        new Object[]{new CliRouter()}
    );
  }

  private final GuiceRunnable runnable;
  public MainTest(GuiceRunnable runnable)
  {
    this.runnable = runnable;
  }
  @Test
  public void testSimpleInjection()
  {
    final Injector injector = GuiceInjectors.makeStartupInjector();
    injector.injectMembers(runnable);
    final Injector runnableInjector = runnable.makeInjector();
    Assert.assertNotNull(runnableInjector);

    NodeTypeConfig nodeTypeConfig = runnableInjector.getInstance(NodeTypeConfig.class);
    Assert.assertEquals(nodeTypeMap.get(runnable.getClass()), nodeTypeConfig.getNodeType());
  }
}
