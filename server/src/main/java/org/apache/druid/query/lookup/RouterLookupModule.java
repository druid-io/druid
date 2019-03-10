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

package org.apache.druid.query.lookup;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.query.dimension.LookupDimensionSpec;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Variant of {@link LookupModule} that only supports serde of {@link org.apache.druid.query.Query} objects, to allow
 * the router to examine queries that might contain for example a {@link RegisteredLookupExtractionFn}, but without
 * requiring the router to load the actual lookups.
 */
public class RouterLookupModule implements DruidModule
{
  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.<Module>of(
        new SimpleModule("DruidLookupModule").registerSubtypes(MapLookupExtractorFactory.class),
        new SimpleModule().registerSubtypes(
            new NamedType(LookupDimensionSpec.class, "lookup"),
            new NamedType(RegisteredLookupExtractionFn.class, "registeredLookup")
        )
    );
  }

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, LookupModule.PROPERTY_BASE, LookupConfig.class);
    binder.bind(LookupExtractorFactoryContainerProvider.class).to(RouterLookupExtractorFactoryContainerProvider.class);
  }

  /**
   * The router doesn't actually need lookups, but the objects that get materialized during deserialization expect
   * a {@link LookupExtractorFactoryContainerProvider} to exist.
   */
  private static class RouterLookupExtractorFactoryContainerProvider implements LookupExtractorFactoryContainerProvider
  {
    @Nullable
    @Override
    public LookupExtractorFactoryContainer get(String lookupName)
    {
      return null;
    }
  }
}
