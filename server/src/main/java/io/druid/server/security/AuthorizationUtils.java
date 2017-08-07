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

package io.druid.server.security;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.druid.java.util.common.ISE;

import javax.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Static utility functions for performing authorization checks.
 */
public class AuthorizationUtils
{
  public final static Access ACCESS_OK = new Access(true, "All resource-actions authorized.");

  /**
   * Check a resource-action using the authorization fields from the request.
   *
   * Otherwise, if the resource-actions is authorized, return ACCESS_OK.
   *
   * This function will set the DRUID_AUTH_TOKEN_CHECKED attribute in the request.
   *
   * If this attribute is already set when this function is called, an exception is thrown.
   *
   * @param request HTTP request to be authorized
   * @param resourceAction A resource identifier and the action to be taken the resource.
   * @param authorizerMapper The singleton AuthorizerMapper instance
   * @return ACCESS_OK or the failed Access object returned by the Authorizer that checked the request.
   */
  public static Access authorizeResourceAction(
      final HttpServletRequest request,
      final ResourceAction resourceAction,
      final AuthorizerMapper authorizerMapper
  )
  {
    return authorizeAllResourceActions(
        request,
        Lists.newArrayList(resourceAction),
        authorizerMapper
    );
  }

  /**
   * Check a list of resource-actions using the authorization fields from the request.
   *
   * If one of the resource-actions fails the authorization check, this method returns the failed
   * Access object from the check.
   *
   * Otherwise, return ACCESS_OK if all resource-actions were successfully authorized.
   *
   * This function will set the DRUID_AUTH_TOKEN_CHECKED attribute in the request.
   *
   * If this attribute is already set when this function is called, an exception is thrown.
   * @param request HTTP request to be authorized
   * @param resourceActions A list of resource-actions to authorize
   * @return ACCESS_OK or the Access object from the first failed check
   */
  public static Access authorizeAllResourceActions(
      final HttpServletRequest request,
      final List<ResourceAction> resourceActions,
      final AuthorizerMapper authorizerMapper
  )
  {
    final String identity = (String) request.getAttribute(AuthConfig.DRUID_AUTH_TOKEN);
    if (identity == null) {
      throw new ISE("Null identity.");
    }

    final String namespace = (String) request.getAttribute(AuthConfig.DRUID_AUTH_NAMESPACE);
    if (namespace == null) {
      throw new ISE("Null namespace.");
    }

    final Authorizer authorizer = authorizerMapper.getAuthorizer(namespace);
    if (authorizer == null) {
      throw new ISE("No authorizer found for namespace: [%s].", namespace);
    }

    // this method returns on first failure, so only successful Access results are kept in the cache
    final Set<ResourceAction> resultCache = Sets.newHashSet();

    for (ResourceAction resourceAction : resourceActions) {
      if (resultCache.contains(resourceAction)) {
        continue;
      }
      final Access access = authorizer.authorize(
          identity,
          resourceAction.getResource(),
          resourceAction.getAction()
      );
      if (!access.isAllowed()) {
        request.setAttribute(AuthConfig.DRUID_AUTH_TOKEN_CHECKED, false);
        return access;
      } else {
        resultCache.add(resourceAction);
      }
    }

    request.setAttribute(AuthConfig.DRUID_AUTH_TOKEN_CHECKED, true);
    return ACCESS_OK;
  }

  /**
   * Check a list of caller-defined resources, after converting them into a list of resource-actions
   * using a caller provided function.
   *
   * If one of the resource-actions fails the authorization check, this method returns the failed
   * Access object from the check.
   *
   * Otherwise, return ACCESS_OK if all resource-actions were successfully authorized.
   *
   * This function will set the DRUID_AUTH_TOKEN_CHECKED attribute in the request.
   *
   * If this attribute is already set when this fImmutableList.<Class<?>>of(SupervisorManager.class, Authorizer.class)unction is called, an exception is thrown.
   *
   * @param request HTTP request to be generated
   * @param resources List of resources
   * @param raGenerator Function that creates a resource-action from a resource
   * @param <ResType> Type of the resources in the resource list
   * @return ACCESS_OK or the Access object from the first failed check
   */
  public static <ResType> Access authorizeAllResourceActions(
      final HttpServletRequest request,
      final Collection<ResType> resources,
      final Function<? super ResType, ResourceAction> raGenerator,
      final AuthorizerMapper authorizerMapper
  )
  {
    final String identity = (String) request.getAttribute(AuthConfig.DRUID_AUTH_TOKEN);
    if (identity == null) {
      throw new ISE("Null identity.");
    }

    final String namespace = (String) request.getAttribute(AuthConfig.DRUID_AUTH_NAMESPACE);
    if (namespace == null) {
      throw new ISE("Null namespace.");
    }

    final Authorizer authorizer = authorizerMapper.getAuthorizer(namespace);
    if (authorizer == null) {
      throw new ISE("No authorizer found for namespace: [%s].", namespace);
    }

    // this method returns on first failure, so only successful Access results are kept in the cache
    final Set<ResourceAction> resultCache = Sets.newHashSet();

    for (ResType resource : resources) {
      final ResourceAction resourceAction = raGenerator.apply(resource);
      if (resultCache.contains(resourceAction)) {
        continue;
      }
      final Access access = authorizer.authorize(
          identity,
          resourceAction.getResource(),
          resourceAction.getAction()
      );
      if (!access.isAllowed()) {
        request.setAttribute(AuthConfig.DRUID_AUTH_TOKEN_CHECKED, false);
        return access;
      } else {
        resultCache.add(resourceAction);
      }
    }

    request.setAttribute(AuthConfig.DRUID_AUTH_TOKEN_CHECKED, true);
    return ACCESS_OK;
  }

  /**
   * Check a list of caller-defined resources, after converting them into a list of resource-actions
   * using a caller provided function.
   *
   * If one of the resource-actions fails the authorization check, this method returns the failed
   * Access object from the check.
   *
   * Otherwise, return ACCESS_OK if all resource-actions were successfully authorized.
   *
   * @param resources List of resources
   * @param raGenerator Function that creates a resource-action from a resource
   * @param <ResType> Type of the resources in the resource list
   * @return ACCESS_OK or the Access object from the first failed check
   */
  public static <ResType> Access authorizeAllResourceActions(
      final Collection<ResType> resources,
      final Function<? super ResType, ResourceAction> raGenerator,
      final String user,
      final String namespace,
      final AuthorizerMapper authorizerMapper
  )
  {
    if (user == null || namespace == null) {
      throw new ISE("null user or namespace");
    }
    final Authorizer authorizer = authorizerMapper.getAuthorizer(namespace);
    if (authorizer == null) {
      throw new ISE("No authorizer found for namespace: [%s].", namespace);
    }

    // this method returns on first failure, so only successful Access results are kept in the cache
    final Set<ResourceAction> resultCache = Sets.newHashSet();

    for (ResType resource : resources) {
      final ResourceAction resourceAction = raGenerator.apply(resource);
      if (resultCache.contains(resourceAction)) {
        continue;
      }
      final Access access = authorizer.authorize(
          user,
          resourceAction.getResource(),
          resourceAction.getAction()
      );
      if (!access.isAllowed()) {
        return access;
      } else {
        resultCache.add(resourceAction);
      }
    }

    return ACCESS_OK;
  }

  /**
   * Filter a list of resource-actions using the request's authorization fields, returning a new list of
   * resource-actions that were authorized.
   *
   * This function will set the DRUID_AUTH_TOKEN_CHECKED attribute in the request.
   *
   * If this attribute is already set when this function is called, an exception is thrown.
   *
   * @param request HTTP request to be authorized
   * @param resources List of resources to be processed into resource-actions
   * @param resourceActionGenerator Function that creates a resource-action from a resource
   * @return A list containing the resource-actions from the resourceParser that were successfully authorized.
   */
  public static <ResType> List<ResType> filterAuthorizedResources(
      final HttpServletRequest request,
      final Collection<ResType> resources,
      final Function<? super ResType, ResourceAction> resourceActionGenerator,
      final AuthorizerMapper authorizerMapper
  )
  {
    final String identity = (String) request.getAttribute(AuthConfig.DRUID_AUTH_TOKEN);
    if (identity == null) {
      throw new ISE("Null identity.");
    }

    final String namespace = (String) request.getAttribute(AuthConfig.DRUID_AUTH_NAMESPACE);
    if (namespace == null) {
      throw new ISE("Null namespace.");
    }

    final Authorizer authorizer = authorizerMapper.getAuthorizer(namespace);
    if (authorizer == null) {
      throw new ISE("No authorizer found for namespace: [%s].", namespace);
    }

    int initialSize = resources.size();
    final Map<ResourceAction, Access> resultCache = Maps.newHashMap();
    List<ResType> filteredResources = new ArrayList<>();
    for (ResType resource : resources) {
      final ResourceAction resourceAction = resourceActionGenerator.apply(resource);
      Access access = resultCache.get(resourceAction);
      if (access == null) {
        access = authorizer.authorize(
            identity,
            resourceAction.getResource(),
            resourceAction.getAction()
        );
        resultCache.put(resourceAction, access);
      }
      if (access.isAllowed()) {
        filteredResources.add(resource);
      }
    }

    request.setAttribute(AuthConfig.DRUID_AUTH_TOKEN_CHECKED, (filteredResources.size() > 0 || initialSize == 0));
    return filteredResources;
  }

  /**
   * Function for the common pattern of generating a resource-action for reading from a datasource, using the
   * datasource name.
   */
  public static Function<String, ResourceAction> DATASOURCE_READ_RA_GENERATOR = new Function<String, ResourceAction>()
  {
    @Override
    public ResourceAction apply(String input)
    {
      return new ResourceAction(
          new Resource(input, ResourceType.DATASOURCE),
          Action.READ
      );
    }
  };

  /**
   * Function for the common pattern of generating a resource-action for reading from a datasource, using the
   * datasource name.
   */
  public static Function<String, ResourceAction> DATASOURCE_WRITE_RA_GENERATOR = new Function<String, ResourceAction>()
  {
    @Override
    public ResourceAction apply(String input)
    {
      return new ResourceAction(
          new Resource(input, ResourceType.DATASOURCE),
          Action.WRITE
      );
    }
  };
}
