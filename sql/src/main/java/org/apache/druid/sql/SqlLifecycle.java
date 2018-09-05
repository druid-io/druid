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

package org.apache.druid.sql;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.SequenceWrapper;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.sql.calcite.planner.DruidPlanner;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.planner.PlannerResult;
import org.apache.druid.sql.http.SqlQuery;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import javax.servlet.http.HttpServletRequest;
import java.util.Map;
import java.util.UUID;

@ThreadSafe
public class SqlLifecycle
{
  private static final Logger log = new Logger(SqlLifecycle.class);

  private final PlannerFactory plannerFactory;
  private final ServiceEmitter emitter;
  private final AuthorizerMapper authorizerMapper;
  private final long startMs;
  private final long startNs;
  private final Object lock = new Object();

  private State state = State.NEW;

  // init during intialize
  private String sql;
  private Map<String, Object> queryContext;
  // init during plan
  private HttpServletRequest req; // may be null
  private PlannerContext plannerContext;
  private PlannerResult plannerResult;

  public SqlLifecycle(
      PlannerFactory plannerFactory,
      ServiceEmitter emitter,
      AuthorizerMapper authorizerMapper,
      long startMs,
      long startNs
  )
  {
    this.plannerFactory = plannerFactory;
    this.emitter = emitter;
    this.authorizerMapper = authorizerMapper;
    this.startMs = startMs;
    this.startNs = startNs;
  }

  public String initialize(SqlQuery sqlQuery)
  {
    synchronized (lock) {
      transition(State.NEW, State.INITIALIZED);
      this.sql = sqlQuery.getQuery();
      this.queryContext = contextWithSqlId(sqlQuery.getContext());
      return sqlId();
    }
  }

  public String initialize(String sql, Map<String, Object> queryContext)
  {
    synchronized (lock) {
      transition(State.NEW, State.INITIALIZED);
      this.sql = sql;
      this.queryContext = contextWithSqlId(queryContext);
      return sqlId();
    }
  }

  private Map<String, Object> contextWithSqlId(Map<String, Object> queryContext)
  {
    Map<String, Object> newContext = Maps.newHashMap();
    if (queryContext != null) {
      newContext.putAll(queryContext);
    }
    if (!newContext.containsKey(PlannerContext.CTX_SQL_ID)) {
      newContext.put(PlannerContext.CTX_SQL_ID, UUID.randomUUID().toString());
    }
    return newContext;
  }

  private String sqlId()
  {
    return (String) this.queryContext.get(PlannerContext.CTX_SQL_ID);
  }

  public PlannerContext plan(AuthenticationResult authenticationResult)
      throws ValidationException, RelConversionException, SqlParseException
  {
    synchronized (lock) {
      transition(State.INITIALIZED, State.PLANED);
      try (DruidPlanner planner = plannerFactory.createPlanner(queryContext, authenticationResult)) {
        this.plannerContext = planner.getPlannerContext();
        this.plannerResult = planner.plan(sql);
      }
      return plannerContext;
    }
  }

  public PlannerContext plan(HttpServletRequest req)
      throws SqlParseException, RelConversionException, ValidationException
  {
    synchronized (lock) {
      this.req = req;
      return plan(AuthorizationUtils.authenticationResultFromRequest(req));
    }
  }

  public RelDataType rowType()
  {
    synchronized (lock) {
      Preconditions.checkState(plannerResult != null,
                               "must be call after sql has been planned");
      return plannerResult.rowType();
    }
  }

  public Access authorize()
  {
    synchronized (lock) {
      transition(State.PLANED, State.AUTHORIZING);

      if (req != null) {
        return doAuthorize(
            plannerContext.getAuthenticationResult(),
            AuthorizationUtils.authorizeAllResourceActions(
                req,
                Iterables.transform(
                    plannerResult.datasourceNames(),
                    AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR
                ),
                authorizerMapper
            )
        );
      }

      return doAuthorize(
          plannerContext.getAuthenticationResult(),
          AuthorizationUtils.authorizeAllResourceActions(
              plannerContext.getAuthenticationResult(),
              Iterables.transform(plannerResult.datasourceNames(), AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR),
              authorizerMapper
          )
      );
    }
  }

  private Access doAuthorize(final AuthenticationResult authenticationResult, final Access authorizationResult)
  {
    if (!authorizationResult.isAllowed()) {
      // Not authorized; go straight to Jail, do not pass Go.
      transition(State.AUTHORIZING, State.UNAUTHORIZED);
    } else {
      transition(State.AUTHORIZING, State.AUTHORIZED);
    }
    return authorizationResult;
  }

  public PlannerContext planAndAuthorize(final AuthenticationResult authenticationResult)
      throws SqlParseException, RelConversionException, ValidationException
  {
    PlannerContext plannerContext = plan(authenticationResult);
    Access access = authorize();
    if (!access.isAllowed()) {
      throw new ForbiddenException(access.toString());
    }
    return plannerContext;
  }

  public PlannerContext planAndAuthorize(final HttpServletRequest req)
      throws SqlParseException, RelConversionException, ValidationException
  {
    PlannerContext plannerContext = plan(req);
    Access access = authorize();
    if (!access.isAllowed()) {
      throw new ForbiddenException(access.toString());
    }
    return plannerContext;
  }

  public Sequence<Object[]> execute()
  {
    synchronized (lock) {
      transition(State.AUTHORIZED, State.EXECUTING);
      return plannerResult.run();
    }
  }

  public Sequence<Object[]> runSimple(
      String sql,
      Map<String, Object> queryContext,
      AuthenticationResult authenticationResult
  ) throws ValidationException, RelConversionException, SqlParseException
  {
    Sequence<Object[]> result;

    initialize(sql, queryContext);
    try {
      planAndAuthorize(authenticationResult);
      result = execute();
    }
    catch (Throwable e) {
      emitLogsAndMetrics(e, null, -1);
      throw e;
    }

    return Sequences.wrap(result, new SequenceWrapper()
    {
      @Override
      public void after(boolean isDone, Throwable thrown)
      {
        emitLogsAndMetrics(thrown, null, -1);
      }
    });
  }

  /**
   * Emit logs and metrics for this query.
   *
   * @param e             exception that occurred while processing this query
   * @param remoteAddress remote address, for logging; or null if unknown
   * @param bytesWritten  number of bytes written; will become a query/bytes metric if >= 0
   */
  public void emitLogsAndMetrics(
      @Nullable final Throwable e,
      @Nullable final String remoteAddress,
      final long bytesWritten
  )
  {
    synchronized (lock) {
      if (sql == null) {
        // Never initialized, don't log or emit anything.
        return;
      }

      if (state == State.DONE) {
        log.warn("Tried to emit logs and metrics twice for query[%s]!", sqlId());
      }

      state = State.DONE;

      final boolean success = e == null;
      final long queryTimeNs = System.nanoTime() - startNs;
      // TODO emit metrics and request log
    }
  }

  private void transition(final State from, final State to)
  {
    if (state != from) {
      throw new ISE("Cannot transition from[%s] to[%s].", from, to);
    }

    state = to;
  }

  enum State
  {
    NEW,
    INITIALIZED,
    PLANED,
    AUTHORIZING,
    AUTHORIZED,
    EXECUTING,
    UNAUTHORIZED,
    DONE
  }
}
