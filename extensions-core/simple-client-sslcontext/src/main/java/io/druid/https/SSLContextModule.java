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

package io.druid.https;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.annotations.Client;
import io.druid.guice.annotations.EscalatedClient;
import io.druid.guice.annotations.EscalatedGlobal;
import io.druid.guice.annotations.Global;
import io.druid.initialization.DruidModule;
import io.druid.server.router.Router;

import javax.net.ssl.SSLContext;
import java.util.List;

public class SSLContextModule implements DruidModule
{

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of();
  }

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.client.https", SSLClientConfig.class);
    binder.bind(SSLContext.class).toProvider(SSLContextProvider.class);
    binder.bind(SSLContext.class).annotatedWith(Global.class).toProvider(SSLContextProvider.class);
    binder.bind(SSLContext.class).annotatedWith(Client.class).toProvider(SSLContextProvider.class);
    binder.bind(SSLContext.class).annotatedWith(Router.class).toProvider(SSLContextProvider.class);
    binder.bind(SSLContext.class).annotatedWith(EscalatedGlobal.class).toProvider(SSLContextProvider.class);
    binder.bind(SSLContext.class).annotatedWith(EscalatedClient.class).toProvider(SSLContextProvider.class);
  }
}
