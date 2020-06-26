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

package org.apache.druid.server.security;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.HttpMethod;
import java.io.IOException;
import java.util.List;
import java.util.Set;

public class AllowHttpMethodsResourceFilter implements Filter
{
  /**
   * DRUID always allows GET, POST, PUT, DELETE and OPTIONS methods.
   *
   * NOTE: the OPTIONS method is allowed because prior to Druid 0.19, this method can be disabled by using the
   * {@link AllowOptionsResourceFilter} and is configurable via {@link AuthConfig#allowUnauthenticatedHttpOptions}.
   */
  @VisibleForTesting
  static final List<String> SUPPORTED_METHODS =
      ImmutableList.of(HttpMethod.GET, HttpMethod.POST, HttpMethod.PUT, HttpMethod.DELETE, HttpMethod.OPTIONS);

  private final Set<String> supportedMethods;

  public AllowHttpMethodsResourceFilter(@Nonnull List<String> additionalSupportedMethods)
  {
    supportedMethods = Sets.newHashSetWithExpectedSize(additionalSupportedMethods.size() + SUPPORTED_METHODS.size());
    supportedMethods.addAll(SUPPORTED_METHODS);
    supportedMethods.addAll(additionalSupportedMethods);
  }

  @Override
  public void init(FilterConfig filterConfig)
  {
    /* Do nothing. */
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException
  {
    HttpServletRequest httpReq = (HttpServletRequest) request;
    if (!supportedMethods.contains(httpReq.getMethod())) {
      ((HttpServletResponse) response).sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED);
    } else {
      chain.doFilter(request, response);
    }
  }

  @Override
  public void destroy()
  {
    /* Do nothing. */
  }
}
