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

package io.druid.server.lookup.cache;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Same as LookupsState except that it uses LookupExtractorFactoryMapContainer instead of
 * LookupExtractorFactoryContainer to refer to lookup specs so that lookup extensions are not required to
 * be loaded at the Coordinator.
 */
public class LookupsStateWithMap
{
  private Map<String, LookupExtractorFactoryMapContainer> current;
  private Map<String, LookupExtractorFactoryMapContainer> toLoad;
  private Set<String> toDrop;

  @JsonCreator
  public LookupsStateWithMap(
      @JsonProperty("current") Map<String, LookupExtractorFactoryMapContainer> current,
      @JsonProperty("toLoad") Map<String, LookupExtractorFactoryMapContainer> toLoad,
      @JsonProperty("toDrop") Set<String> toDrop
  )
  {
    this.current = current == null ? Collections.EMPTY_MAP : current;
    this.toLoad = toLoad == null ? Collections.EMPTY_MAP : toLoad;
    this.toDrop = toDrop == null ? Collections.EMPTY_SET : toDrop;
  }

  @JsonProperty
  public Map<String, LookupExtractorFactoryMapContainer> getCurrent()
  {
    return current;
  }

  @JsonProperty
  public Map<String, LookupExtractorFactoryMapContainer> getToLoad()
  {
    return toLoad;
  }

  @JsonProperty
  public Set<String> getToDrop()
  {
    return toDrop;
  }

  @Override
  public String toString()
  {
    return "LookupsState{" +
           "current=" + current +
           ", toLoad=" + toLoad +
           ", toDrop=" + toDrop +
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
    LookupsStateWithMap that = (LookupsStateWithMap) o;
    return Objects.equals(current, that.current) &&
           Objects.equals(toLoad, that.toLoad) &&
           Objects.equals(toDrop, that.toDrop);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(current, toLoad, toDrop);
  }
}
