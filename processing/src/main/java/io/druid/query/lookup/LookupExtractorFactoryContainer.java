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

package io.druid.query.lookup;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

/**
 */
public class LookupExtractorFactoryContainer
{
  private String version;
  private LookupExtractorFactory lookupExtractorFactory;

  @JsonCreator
  public LookupExtractorFactoryContainer(
      @JsonProperty("version") String version,
      @JsonProperty("lookupExtractorFactory") LookupExtractorFactory lookupExtractorFactory
  )
  {
    this.version = version;
    this.lookupExtractorFactory = Preconditions.checkNotNull(lookupExtractorFactory, "null factory");
  }

  @JsonProperty
  public String getVersion()
  {
    return version;
  }

  @JsonProperty
  public LookupExtractorFactory getLookupExtractorFactory()
  {
    return lookupExtractorFactory;
  }

  public boolean replaces(LookupExtractorFactoryContainer other) {
    if (version == null && other.getVersion() == null) {
      return !this.lookupExtractorFactory.equals(other.getLookupExtractorFactory());
    }

    if (version == null && other.getVersion() != null) {
      return false;
    }

    if (version != null && other.getVersion() == null) {
      return true;
    }

    return version.compareTo(other.getVersion()) > 0;
  }

  @Override
  public String toString()
  {
    return "LookupExtractorFactoryContainer{" +
           "version='" + version + '\'' +
           ", lookupExtractorFactory=" + lookupExtractorFactory +
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

    LookupExtractorFactoryContainer that = (LookupExtractorFactoryContainer) o;

    if (version != null ? !version.equals(that.version) : that.version != null) {
      return false;
    }
    return lookupExtractorFactory.equals(that.lookupExtractorFactory);

  }

  @Override
  public int hashCode()
  {
    int result = version != null ? version.hashCode() : 0;
    result = 31 * result + lookupExtractorFactory.hashCode();
    return result;
  }
}
