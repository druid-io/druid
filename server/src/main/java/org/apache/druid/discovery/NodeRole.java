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

package org.apache.druid.discovery;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This is a historical occasion that this enum is different from {@link
 * org.apache.druid.server.coordination.ServerType} (also called "node type" in various places) because they are
 * essentially the same abstraction, but merging them could only increase the complexity and drop the code safety,
 * because they name the same types differently ("peon" - "indexer-executor" and "middleManager" - "realtime") and both
 * expose them via JSON APIs.
 *
 * These abstractions can probably be merged when Druid updates to Jackson 2.9 that supports JsonAliases, see
 * see https://github.com/apache/druid/issues/7152.
 */
public class NodeRole
{
  public static final NodeRole COORDINATOR = new NodeRole("COORDINATOR", "coordinator");
  public static final NodeRole HISTORICAL = new NodeRole("HISTORICAL", "historical");
  public static final NodeRole BROKER = new NodeRole("BROKER", "broker");
  public static final NodeRole OVERLORD = new NodeRole("OVERLORD", "overlord");
  public static final NodeRole PEON = new NodeRole("PEON", "peon");
  public static final NodeRole ROUTER = new NodeRole("ROUTER", "router");
  public static final NodeRole MIDDLE_MANAGER = new NodeRole("MIDDLE_MANAGER", "middleManager");
  public static final NodeRole INDEXER = new NodeRole("INDEXER", "indexer");

  private static final NodeRole[] BUILT_IN = new NodeRole[]{
      COORDINATOR,
      HISTORICAL,
      BROKER,
      OVERLORD,
      PEON,
      ROUTER,
      MIDDLE_MANAGER,
      INDEXER
  };

  private static final Map<String, NodeRole> BUILT_IN_LOOKUP =
      Arrays.stream(BUILT_IN).collect(Collectors.toMap(NodeRole::getJsonName, Function.identity()));

  private final String name;
  private final String jsonName;

  public NodeRole(String jsonName)
  {
    this(jsonName, jsonName);
  }

  /**
   * for built-in roles, to preserve backwards compatibility when this was an enum
   */
  private NodeRole(String name, String jsonName)
  {
    this.name = name;
    this.jsonName = jsonName;
  }

  @JsonValue
  public String getJsonName()
  {
    return jsonName;
  }

  @JsonCreator
  public static NodeRole fromJsonName(String jsonName)
  {
    return BUILT_IN_LOOKUP.getOrDefault(jsonName, new NodeRole(jsonName));
  }

  @Override
  public String toString()
  {
    // for built-in roles, to preserve backwards compatibility when this was an enum
    return name;
  }

  public static NodeRole[] values()
  {
    return BUILT_IN;
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
    NodeRole nodeRole = (NodeRole) o;
    return name.equals(nodeRole.name) && jsonName.equals(nodeRole.jsonName);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, jsonName);
  }
}
