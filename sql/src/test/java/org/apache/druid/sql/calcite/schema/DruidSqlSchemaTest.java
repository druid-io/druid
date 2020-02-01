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

package org.apache.druid.sql.calcite.schema;

import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(EasyMockRunner.class)
public class DruidSqlSchemaTest
{
  private static final String SCHEMA_NAME = "SCHEMA_NAME";

  @Mock
  private DruidSchema druidSchema;

  private DruidSqlSchema target;

  @Before
  public void setUp()
  {
    target = new DruidSqlSchema(druidSchema, SCHEMA_NAME);
  }

  @Test
  public void testGetSchemaNameShouldReturnName()
  {
    Assert.assertEquals(SCHEMA_NAME, target.getSchemaName());
  }

  @Test
  public void testGetSchemaShouldReturnSchema()
  {
    Assert.assertEquals(druidSchema, target.getSchema());
  }
}
