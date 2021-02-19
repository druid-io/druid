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

package org.apache.druid.tests.security;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.CredentialedHttpClient;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.auth.BasicCredentials;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.security.basic.authentication.entity.BasicAuthenticatorCredentialUpdate;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.sql.avatica.DruidAvaticaHandler;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.testing.utils.HttpUtil;
import org.apache.druid.testing.utils.ITRetryUtil;
import org.apache.druid.testing.utils.TestQueryHelper;
import org.apache.druid.tests.TestNGGroup;
import org.apache.druid.tests.indexer.AbstractIndexerTest;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Test(groups = TestNGGroup.SECURITY)
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITBasicAuthConfigurationTest extends AbstractAuthConfigurationTest
{
  private static final Logger LOG = new Logger(ITBasicAuthConfigurationTest.class);

  private static final String BASIC_AUTHENTICATOR = "basic";
  private static final String BASIC_AUTHORIZER = "basic";

  private static final String EXPECTED_AVATICA_AUTH_ERROR = "Error while executing SQL \"SELECT * FROM INFORMATION_SCHEMA.COLUMNS\": Remote driver error: BasicSecurityAuthenticationException: User metadata store authentication failed.";

  private static final TypeReference<List<Map<String, Object>>> SYS_SCHEMA_RESULTS_TYPE_REFERENCE =
      new TypeReference<List<Map<String, Object>>>()
      {
      };

  @BeforeMethod
  public void before()
  {
    // ensure that auth_test segments are loaded completely, we use them for testing system schema tables
    ITRetryUtil.retryUntilTrue(
        () -> coordinatorClient.areSegmentsLoaded("auth_test"), "auth_test segment load"
    );
  }

  @Test
  public void testSystemSchemaAccess() throws Exception
  {
    HttpClient adminClient = new CredentialedHttpClient(
        new BasicCredentials("admin", "priest"),
        httpClient
    );

    // check that admin access works on all nodes
    checkNodeAccess(adminClient);

    // create a new user+role that can only read 'auth_test'
    List<ResourceAction> readDatasourceOnlyPermissions = Collections.singletonList(
        new ResourceAction(
            new Resource("auth_test", ResourceType.DATASOURCE),
            Action.READ
        )
    );
    createUserAndRoleWithPermissions(
        adminClient,
        "datasourceOnlyUser",
        "helloworld",
        "datasourceOnlyRole",
        readDatasourceOnlyPermissions
    );
    HttpClient datasourceOnlyUserClient = new CredentialedHttpClient(
        new BasicCredentials("datasourceOnlyUser", "helloworld"),
        httpClient
    );

    // create a new user+role that can only read 'auth_test' + STATE read access
    List<ResourceAction> readDatasourceWithStatePermissions = ImmutableList.of(
        new ResourceAction(
            new Resource("auth_test", ResourceType.DATASOURCE),
            Action.READ
        ),
        new ResourceAction(
            new Resource(".*", ResourceType.STATE),
            Action.READ
        )
    );
    createUserAndRoleWithPermissions(
        adminClient,
        "datasourceWithStateUser",
        "helloworld",
        "datasourceWithStateRole",
        readDatasourceWithStatePermissions
    );
    HttpClient datasourceWithStateUserClient = new CredentialedHttpClient(
        new BasicCredentials("datasourceWithStateUser", "helloworld"),
        httpClient
    );

    // create a new user+role with only STATE read access
    List<ResourceAction> stateOnlyPermissions = ImmutableList.of(
        new ResourceAction(
            new Resource(".*", ResourceType.STATE),
            Action.READ
        )
    );
    createUserAndRoleWithPermissions(
        adminClient,
        "stateOnlyUser",
        "helloworld",
        "stateOnlyRole",
        stateOnlyPermissions
    );
    HttpClient stateOnlyUserClient = new CredentialedHttpClient(
        new BasicCredentials("stateOnlyUser", "helloworld"),
        httpClient
    );

    // check that we can access a datasource-permission restricted resource on the broker
    HttpUtil.makeRequest(
        datasourceOnlyUserClient,
        HttpMethod.GET,
        config.getBrokerUrl() + "/druid/v2/datasources/auth_test",
        null
    );

    // check that we can access a state-permission restricted resource on the broker
    HttpUtil.makeRequest(
        datasourceWithStateUserClient,
        HttpMethod.GET,
        config.getBrokerUrl() + "/status",
        null
    );
    HttpUtil.makeRequest(stateOnlyUserClient, HttpMethod.GET, config.getBrokerUrl() + "/status", null);

    // initial setup is done now, run the system schema response content tests
    final List<Map<String, Object>> adminSegments = jsonMapper.readValue(
        TestQueryHelper.class.getResourceAsStream(SYSTEM_SCHEMA_SEGMENTS_RESULTS_RESOURCE),
        SYS_SCHEMA_RESULTS_TYPE_REFERENCE
    );

    final List<Map<String, Object>> adminServerSegments = jsonMapper.readValue(
        fillSegementServersTemplate(
            config,
            AbstractIndexerTest.getResourceAsString(SYSTEM_SCHEMA_SERVER_SEGMENTS_RESULTS_RESOURCE)
        ),
        SYS_SCHEMA_RESULTS_TYPE_REFERENCE
    );

    final List<Map<String, Object>> adminServers = getServersWithoutCurrentSize(
        jsonMapper.readValue(
            fillServersTemplate(
                config,
                AbstractIndexerTest.getResourceAsString(SYSTEM_SCHEMA_SERVERS_RESULTS_RESOURCE)
            ),
            SYS_SCHEMA_RESULTS_TYPE_REFERENCE
        )
    );

    final List<Map<String, Object>> adminTasks = jsonMapper.readValue(
        TestQueryHelper.class.getResourceAsStream(SYSTEM_SCHEMA_TASKS_RESULTS_RESOURCE),
        SYS_SCHEMA_RESULTS_TYPE_REFERENCE
    );

    // as admin
    LOG.info("Checking sys.segments query as admin...");
    verifySystemSchemaQuery(
        adminClient,
        SYS_SCHEMA_SEGMENTS_QUERY,
        adminSegments
    );

    LOG.info("Checking sys.servers query as admin...");
    verifySystemSchemaServerQuery(
        adminClient,
        SYS_SCHEMA_SERVERS_QUERY,
        getServersWithoutCurrentSize(adminServers)
    );

    LOG.info("Checking sys.server_segments query as admin...");
    verifySystemSchemaQuery(
        adminClient,
        SYS_SCHEMA_SERVER_SEGMENTS_QUERY,
        adminServerSegments
    );

    LOG.info("Checking sys.tasks query as admin...");
    verifySystemSchemaQuery(
        adminClient,
        SYS_SCHEMA_TASKS_QUERY,
        adminTasks
    );

    // as user that can only read auth_test
    LOG.info("Checking sys.segments query as datasourceOnlyUser...");
    verifySystemSchemaQuery(
        datasourceOnlyUserClient,
        SYS_SCHEMA_SEGMENTS_QUERY,
        adminSegments.stream()
                     .filter((segmentEntry) -> "auth_test".equals(segmentEntry.get("datasource")))
                     .collect(Collectors.toList())
    );

    LOG.info("Checking sys.servers query as datasourceOnlyUser...");
    verifySystemSchemaQueryFailure(
        datasourceOnlyUserClient,
        SYS_SCHEMA_SERVERS_QUERY,
        HttpResponseStatus.FORBIDDEN,
        "{\"Access-Check-Result\":\"Insufficient permission to view servers : Allowed:false, Message:\"}"
    );

    LOG.info("Checking sys.server_segments query as datasourceOnlyUser...");
    verifySystemSchemaQueryFailure(
        datasourceOnlyUserClient,
        SYS_SCHEMA_SERVER_SEGMENTS_QUERY,
        HttpResponseStatus.FORBIDDEN,
        "{\"Access-Check-Result\":\"Insufficient permission to view servers : Allowed:false, Message:\"}"
    );

    LOG.info("Checking sys.tasks query as datasourceOnlyUser...");
    verifySystemSchemaQuery(
        datasourceOnlyUserClient,
        SYS_SCHEMA_TASKS_QUERY,
        adminTasks.stream()
                  .filter((taskEntry) -> "auth_test".equals(taskEntry.get("datasource")))
                  .collect(Collectors.toList())
    );

    // as user that can read auth_test and STATE
    LOG.info("Checking sys.segments query as datasourceWithStateUser...");
    verifySystemSchemaQuery(
        datasourceWithStateUserClient,
        SYS_SCHEMA_SEGMENTS_QUERY,
        adminSegments.stream()
                     .filter((segmentEntry) -> "auth_test".equals(segmentEntry.get("datasource")))
                     .collect(Collectors.toList())
    );

    LOG.info("Checking sys.servers query as datasourceWithStateUser...");
    verifySystemSchemaServerQuery(
        datasourceWithStateUserClient,
        SYS_SCHEMA_SERVERS_QUERY,
        adminServers
    );

    LOG.info("Checking sys.server_segments query as datasourceWithStateUser...");
    verifySystemSchemaQuery(
        datasourceWithStateUserClient,
        SYS_SCHEMA_SERVER_SEGMENTS_QUERY,
        adminServerSegments.stream()
                           .filter((serverSegmentEntry) -> ((String) serverSegmentEntry.get("segment_id")).contains(
                               "auth_test"))
                           .collect(Collectors.toList())
    );

    LOG.info("Checking sys.tasks query as datasourceWithStateUser...");
    verifySystemSchemaQuery(
        datasourceWithStateUserClient,
        SYS_SCHEMA_TASKS_QUERY,
        adminTasks.stream()
                  .filter((taskEntry) -> "auth_test".equals(taskEntry.get("datasource")))
                  .collect(Collectors.toList())
    );

    // as user that can only read STATE
    LOG.info("Checking sys.segments query as stateOnlyUser...");
    verifySystemSchemaQuery(
        stateOnlyUserClient,
        SYS_SCHEMA_SEGMENTS_QUERY,
        Collections.emptyList()
    );

    LOG.info("Checking sys.servers query as stateOnlyUser...");
    verifySystemSchemaServerQuery(
        stateOnlyUserClient,
        SYS_SCHEMA_SERVERS_QUERY,
        adminServers
    );

    LOG.info("Checking sys.server_segments query as stateOnlyUser...");
    verifySystemSchemaQuery(
        stateOnlyUserClient,
        SYS_SCHEMA_SERVER_SEGMENTS_QUERY,
        Collections.emptyList()
    );

    LOG.info("Checking sys.tasks query as stateOnlyUser...");
    verifySystemSchemaQuery(
        stateOnlyUserClient,
        SYS_SCHEMA_TASKS_QUERY,
        Collections.emptyList()
    );
  }

  @Test
  public void testAuthConfiguration() throws Exception
  {
    HttpClient adminClient = new CredentialedHttpClient(
        new BasicCredentials("admin", "priest"),
        httpClient
    );

    HttpClient internalSystemClient = new CredentialedHttpClient(
        new BasicCredentials("druid_system", "warlock"),
        httpClient
    );

    HttpClient newUserClient = new CredentialedHttpClient(
        new BasicCredentials("druid", "helloworld"),
        httpClient
    );

    final HttpClient unsecuredClient = httpClient;

    // check that we are allowed to access unsecured path without credentials.
    checkUnsecuredCoordinatorLoadQueuePath(unsecuredClient);

    // check that admin works
    checkNodeAccess(adminClient);

    // check that internal user works
    checkNodeAccess(internalSystemClient);

    // create a new user+role that can read /status
    List<ResourceAction> permissions = Collections.singletonList(
        new ResourceAction(
            new Resource(".*", ResourceType.STATE),
            Action.READ
        )
    );
    createUserAndRoleWithPermissions(
        adminClient,
        "druid",
        "helloworld",
        "druidrole",
        permissions
    );

    // check that the new user works
    checkNodeAccess(newUserClient);

    // check loadStatus
    checkLoadStatus(adminClient, BASIC_AUTHENTICATOR, BASIC_AUTHORIZER);

    // create 100 users
    for (int i = 0; i < 100; i++) {
      HttpUtil.makeRequest(
          adminClient,
          HttpMethod.POST,
          config.getCoordinatorUrl() + "/druid-ext/basic-security/authentication/db/basic/users/druid" + i,
          null
      );

      HttpUtil.makeRequest(
          adminClient,
          HttpMethod.POST,
          config.getCoordinatorUrl() + "/druid-ext/basic-security/authorization/db/basic/users/druid" + i,
          null
      );

      LOG.info("Finished creating user druid" + i);
    }

    // setup the last of 100 users and check that it works
    HttpUtil.makeRequest(
        adminClient,
        HttpMethod.POST,
        config.getCoordinatorUrl() + "/druid-ext/basic-security/authentication/db/basic/users/druid99/credentials",
        jsonMapper.writeValueAsBytes(new BasicAuthenticatorCredentialUpdate("helloworld", 5000))
    );

    HttpUtil.makeRequest(
        adminClient,
        HttpMethod.POST,
        config.getCoordinatorUrl() + "/druid-ext/basic-security/authorization/db/basic/users/druid99/roles/druidrole",
        null
    );

    HttpClient newUser99Client = new CredentialedHttpClient(
        new BasicCredentials("druid99", "helloworld"),
        httpClient
    );

    LOG.info("Checking access for user druid99.");
    checkNodeAccess(newUser99Client);

    String brokerUrl = "jdbc:avatica:remote:url=" + config.getBrokerUrl() + DruidAvaticaHandler.AVATICA_PATH;
    String routerUrl = "jdbc:avatica:remote:url=" + config.getRouterUrl() + DruidAvaticaHandler.AVATICA_PATH;

    LOG.info("Checking Avatica query on broker.");
    testAvaticaQuery(brokerUrl);

    LOG.info("Checking Avatica query on router.");
    testAvaticaQuery(routerUrl);

    LOG.info("Testing Avatica query on broker with incorrect credentials.");
    testAvaticaAuthFailure(brokerUrl, EXPECTED_AVATICA_AUTH_ERROR);

    LOG.info("Testing Avatica query on router with incorrect credentials.");
    testAvaticaAuthFailure(routerUrl, EXPECTED_AVATICA_AUTH_ERROR);

    LOG.info("Checking OPTIONS requests on services...");
    testOptionsRequests(adminClient);
  }

  @Test
  public void testInvalidAuthNames()
  {
    String invalidName = "invalid%2Fname";
    HttpClient adminClient = new CredentialedHttpClient(
        new BasicCredentials("admin", "priest"),
        httpClient
    );

    HttpUtil.makeRequestWithExpectedStatus(
        adminClient,
        HttpMethod.POST,
        StringUtils.format(
            "%s/druid-ext/basic-security/authentication/listen/%s",
            config.getCoordinatorUrl(),
            invalidName
        ),
        "SERIALIZED_DATA".getBytes(StandardCharsets.UTF_8),
        HttpResponseStatus.INTERNAL_SERVER_ERROR
    );

    HttpUtil.makeRequestWithExpectedStatus(
        adminClient,
        HttpMethod.POST,
        StringUtils.format(
            "%s/druid-ext/basic-security/authorization/listen/users/%s",
            config.getCoordinatorUrl(),
            invalidName
        ),
        "SERIALIZED_DATA".getBytes(StandardCharsets.UTF_8),
        HttpResponseStatus.INTERNAL_SERVER_ERROR
    );

    HttpUtil.makeRequestWithExpectedStatus(
        adminClient,
        HttpMethod.POST,
        StringUtils.format(
            "%s/druid-ext/basic-security/authorization/listen/groupMappings/%s",
            config.getCoordinatorUrl(),
            invalidName
        ),
        "SERIALIZED_DATA".getBytes(StandardCharsets.UTF_8),
        HttpResponseStatus.INTERNAL_SERVER_ERROR
    );
  }

  @Test
  public void testMaliciousUser()
  {
    String maliciousUsername = "<script>alert('hello')</script>";
    HttpClient maliciousClient = new CredentialedHttpClient(
        new BasicCredentials(maliciousUsername, "noPass"),
        httpClient
    );
    StatusResponseHolder responseHolder = HttpUtil.makeRequestWithExpectedStatus(
        maliciousClient,
        HttpMethod.GET,
        config.getBrokerUrl() + "/status",
        null,
        HttpResponseStatus.UNAUTHORIZED
    );
    String responseContent = responseHolder.getContent();
    Assert.assertTrue(responseContent.contains("<tr><th>MESSAGE:</th><td>Unauthorized</td></tr>"));
    Assert.assertFalse(responseContent.contains(maliciousUsername));
  }

  private void createUserAndRoleWithPermissions(
      HttpClient adminClient,
      String user,
      String password,
      String role,
      List<ResourceAction> permissions
  ) throws Exception
  {
    HttpUtil.makeRequest(
        adminClient,
        HttpMethod.POST,
        StringUtils.format(
            "%s/druid-ext/basic-security/authentication/db/basic/users/%s",
            config.getCoordinatorUrl(),
            user
        ),
        null
    );
    HttpUtil.makeRequest(
        adminClient,
        HttpMethod.POST,
        StringUtils.format(
            "%s/druid-ext/basic-security/authentication/db/basic/users/%s/credentials",
            config.getCoordinatorUrl(),
            user
        ),
        jsonMapper.writeValueAsBytes(new BasicAuthenticatorCredentialUpdate(password, 5000))
    );
    HttpUtil.makeRequest(
        adminClient,
        HttpMethod.POST,
        StringUtils.format(
            "%s/druid-ext/basic-security/authorization/db/basic/users/%s",
            config.getCoordinatorUrl(),
            user
        ),
        null
    );
    HttpUtil.makeRequest(
        adminClient,
        HttpMethod.POST,
        StringUtils.format(
            "%s/druid-ext/basic-security/authorization/db/basic/roles/%s",
            config.getCoordinatorUrl(),
            role
        ),
        null
    );
    HttpUtil.makeRequest(
        adminClient,
        HttpMethod.POST,
        StringUtils.format(
            "%s/druid-ext/basic-security/authorization/db/basic/users/%s/roles/%s",
            config.getCoordinatorUrl(),
            user,
            role
        ),
        null
    );
    byte[] permissionsBytes = jsonMapper.writeValueAsBytes(permissions);
    HttpUtil.makeRequest(
        adminClient,
        HttpMethod.POST,
        StringUtils.format(
            "%s/druid-ext/basic-security/authorization/db/basic/roles/%s/permissions",
            config.getCoordinatorUrl(),
            role
        ),
        permissionsBytes
    );
  }
}
