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

package org.apache.druid.indexing.overlord.sampler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class SamplerResponseTest
{
  private static final ObjectMapper MAPPER = TestHelper.makeJsonMapper();

  @Test
  public void testSerde() throws IOException
  {
    List<SamplerResponse.SamplerResponseRow> data = ImmutableList.of(
        new SamplerResponse.SamplerResponseRow(
            "parsed1",
            ImmutableMap.of("t", 123456, "dim1", "foo", "met1", 6),
            null,
            null
        ),
        new SamplerResponse.SamplerResponseRow(
            "parsed2",
            ImmutableMap.of("t", 123457, "dim1", "foo2", "met1", 7),
            null,
            null
        ),
        new SamplerResponse.SamplerResponseRow("unparsed", null, true, "Could not parse")
    );

    String out = MAPPER.writeValueAsString(new SamplerResponse("eaebbfd87ec34bc6a9f8c03ecee4dd7a", 1123, 1112, data));
    String expected = "{\"cacheKey\":\"eaebbfd87ec34bc6a9f8c03ecee4dd7a\",\"numRowsRead\":1123,\"numRowsIndexed\":1112,\"data\":[{\"raw\":\"parsed1\",\"parsed\":{\"t\":123456,\"dim1\":\"foo\",\"met1\":6}},{\"raw\":\"parsed2\",\"parsed\":{\"t\":123457,\"dim1\":\"foo2\",\"met1\":7}},{\"raw\":\"unparsed\",\"unparseable\":true,\"error\":\"Could not parse\"}]}";

    Assert.assertEquals(expected, out);
  }
}
