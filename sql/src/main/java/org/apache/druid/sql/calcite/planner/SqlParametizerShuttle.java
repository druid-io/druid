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

package org.apache.druid.sql.calcite.planner;

import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.util.TimestampString;

public class SqlParametizerShuttle extends SqlShuttle
{
  private final PlannerContext plannerContext;

  public SqlParametizerShuttle(PlannerContext plannerContext)
  {
    this.plannerContext = plannerContext;
  }

  @Override
  public SqlNode visit(SqlDynamicParam param)
  {
    try {
      if (plannerContext.getParameters().size() > param.getIndex()) {
        TypedValue paramBinding = plannerContext.getParameters().get(param.getIndex());
        SqlTypeName typeName = SqlTypeName.getNameForJdbcType(paramBinding.type.typeId);
        if (SqlTypeName.APPROX_TYPES.contains(typeName)) {
          return SqlLiteral.createApproxNumeric(paramBinding.value.toString(), param.getParserPosition());
        }
        if (SqlTypeName.TIMESTAMP.equals(typeName) && paramBinding.value instanceof Long) {
          return SqlLiteral.createTimestamp(
              TimestampString.fromMillisSinceEpoch((Long) paramBinding.value),
              0,
              param.getParserPosition()
          );
        }
        return typeName.createLiteral(paramBinding.value, param.getParserPosition());
      }
    }
    catch (ClassCastException ignored) {
      // suppress
    }
    return param;
  }
}
