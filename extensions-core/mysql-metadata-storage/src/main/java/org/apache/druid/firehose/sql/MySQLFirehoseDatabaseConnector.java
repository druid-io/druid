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

package org.apache.druid.firehose.sql;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.metadata.SQLFirehoseDatabaseConnector;
import org.apache.druid.server.initialization.JdbcAccessSecurityConfig;
import org.apache.druid.utils.ConnectionUriUtils;
import org.skife.jdbi.v2.DBI;

import javax.annotation.Nullable;
import java.util.Set;


@JsonTypeName("mysql")
public class MySQLFirehoseDatabaseConnector extends SQLFirehoseDatabaseConnector
{
  private final DBI dbi;
  private final MetadataStorageConnectorConfig connectorConfig;

  @JsonCreator
  public MySQLFirehoseDatabaseConnector(
      @JsonProperty("connectorConfig") MetadataStorageConnectorConfig connectorConfig,
      @JsonProperty("driverClassName") @Nullable String driverClassName,
      @JacksonInject JdbcAccessSecurityConfig securityConfig
  )
  {
    this.connectorConfig = connectorConfig;
    final BasicDataSource datasource = getDatasource(connectorConfig, securityConfig);
    datasource.setDriverClassLoader(getClass().getClassLoader());
    datasource.setDriverClassName(driverClassName != null ? driverClassName : "com.mysql.jdbc.Driver");
    this.dbi = new DBI(datasource);
  }

  @JsonProperty
  public MetadataStorageConnectorConfig getConnectorConfig()
  {
    return connectorConfig;
  }

  @Override
  public DBI getDBI()
  {
    return dbi;
  }

  @Override
  public Set<String> findPropertyKeysFromConnectURL(String connectUrl, boolean allowUnknown)
  {
    return ConnectionUriUtils.tryParseJdbcUriParameters(connectUrl, allowUnknown);
  }
}
