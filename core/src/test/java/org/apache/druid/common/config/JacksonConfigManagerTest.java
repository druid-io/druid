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

package org.apache.druid.common.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.audit.AuditEntry;
import org.apache.druid.audit.AuditInfo;
import org.apache.druid.audit.AuditManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Objects;

@RunWith(MockitoJUnitRunner.class)
public class JacksonConfigManagerTest
{
  @Mock
  private ConfigManager mockConfigManager;

  @Mock
  private AuditManager mockAuditManager;

  private JacksonConfigManager jacksonConfigManager;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Before
  public void setUp()
  {
    jacksonConfigManager = new JacksonConfigManager(
        mockConfigManager,
        new ObjectMapper(),
        new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL),
        mockAuditManager
    );
  }

  @Test
  public void testSerializeToStringWithSkipNullTrue()
  {
    ConfigSerde<TestConfig> configConfigSerdeFromTypeReference = jacksonConfigManager.create(new TypeReference<TestConfig>()
    {
    }, null);
    ConfigSerde<TestConfig> configConfigSerdeFromClass = jacksonConfigManager.create(TestConfig.class, null);
    TestConfig config = new TestConfig("version", null, 3);
    String actual = configConfigSerdeFromTypeReference.serializeToString(config, true);
    Assert.assertEquals("{\"version\":\"version\",\"settingInt\":3}", actual);
    actual = configConfigSerdeFromClass.serializeToString(config, true);
    Assert.assertEquals("{\"version\":\"version\",\"settingInt\":3}", actual);
  }

  @Test
  public void testSerializeToStringWithSkipNullFalse()
  {
    ConfigSerde<TestConfig> configConfigSerdeFromTypeReference = jacksonConfigManager.create(new TypeReference<TestConfig>()
    {
    }, null);
    ConfigSerde<TestConfig> configConfigSerdeFromClass = jacksonConfigManager.create(TestConfig.class, null);
    TestConfig config = new TestConfig("version", null, 3);
    String actual = configConfigSerdeFromTypeReference.serializeToString(config, false);
    Assert.assertEquals("{\"version\":\"version\",\"settingString\":null,\"settingInt\":3}", actual);
    actual = configConfigSerdeFromClass.serializeToString(config, false);
    Assert.assertEquals("{\"version\":\"version\",\"settingString\":null,\"settingInt\":3}", actual);
  }

  @Test
  public void testSetWithInvalidConfigForConfigSerdeFromTypeReference()
  {
    ConfigSerde<ClassThatJacksonCannotSerialize> configConfigSerdeFromTypeReference = jacksonConfigManager.create(new TypeReference<ClassThatJacksonCannotSerialize>()
    {
    }, null);
    exception.expect(RuntimeException.class);
    exception.expectMessage("InvalidDefinitionException");
    configConfigSerdeFromTypeReference.serializeToString(new ClassThatJacksonCannotSerialize(), false);
  }

  @Test
  public void testSetWithInvalidConfigForConfigSerdeFromClass()
  {
    ConfigSerde<ClassThatJacksonCannotSerialize> configConfigSerdeFromClass = jacksonConfigManager.create(ClassThatJacksonCannotSerialize.class, null);
    exception.expect(RuntimeException.class);
    exception.expectMessage("InvalidDefinitionException");
    configConfigSerdeFromClass.serializeToString(new ClassThatJacksonCannotSerialize(), false);
  }


  static class TestConfig
  {
    private final String version;
    private final String settingString;
    private final int settingInt;

    @JsonCreator
    public TestConfig(
        @JsonProperty("version") String version,
        @JsonProperty("settingString") String settingString,
        @JsonProperty("settingInt") int settingInt
    )
    {
      this.version = version;
      this.settingString = settingString;
      this.settingInt = settingInt;
    }

    public String getVersion()
    {
      return version;
    }

    public String getSettingString()
    {
      return settingString;
    }

    public int getSettingInt()
    {
      return settingInt;
    }

    @Override
    public String toString()
    {
      return "TestConfig{" +
             "version='" + version + '\'' +
             ", settingString='" + settingString + '\'' +
             ", settingInt=" + settingInt +
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
      TestConfig that = (TestConfig) o;
      return settingInt == that.settingInt &&
             Objects.equals(version, that.version) &&
             Objects.equals(settingString, that.settingString);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(version, settingString, settingInt);
    }
  }

  static class ClassThatJacksonCannotSerialize
  {

  }
}
