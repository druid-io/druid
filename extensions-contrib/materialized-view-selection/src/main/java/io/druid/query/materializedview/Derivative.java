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

package io.druid.query.materializedview;

import java.util.Objects;
import java.util.Set;

public class Derivative implements Comparable<Derivative>
{
  private final String name;
  private final Set<String> fields;
  private final long avgSizeBasedGranularity;

  public Derivative(String name, Set<String> fields, long size)
  {
    this.name = name;
    this.fields = fields;
    this.avgSizeBasedGranularity = size;
  }

  public String getName()
  {
    return name;
  }

  public Set<String> getFields()
  {
    return fields;
  }

  public long getAvgSizeBasedGranularity()
  {
    return avgSizeBasedGranularity;
  }

  @Override
  public int compareTo(Derivative o)
  {
    if (this.avgSizeBasedGranularity > o.getAvgSizeBasedGranularity()) {
      return 1;
    } else if (this.avgSizeBasedGranularity == o.getAvgSizeBasedGranularity()) {
      return 0;
    } else {
      return -1;
    }
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == null) {
      return false;
    }
    if (!(o instanceof Derivative)) {
      return false;
    }
    Derivative that = (Derivative) o;
    return this.name.equals(that.getName()) && this.fields.equals(that.getFields());
  }

  @Override
  public int hashCode()
  {
    return Objects.hashCode(name) + Objects.hashCode(fields);
  }
}
