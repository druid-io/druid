/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.query.lookup;

import com.google.common.util.concurrent.Service.State;

import javax.ws.rs.GET;
import javax.ws.rs.core.Response;

public class KafkaLookupExtractorIntrospectionHandler implements LookupIntrospectHandler
{
  private KafkaLookupExtractorFactory kafkaLookupExtractorFactory;

  public KafkaLookupExtractorIntrospectionHandler(KafkaLookupExtractorFactory kafkaLookupExtractorFactory) {this.kafkaLookupExtractorFactory = kafkaLookupExtractorFactory;}

  @GET
  public Response getActive()
  {
    final State state = kafkaLookupExtractorFactory.getState();
    // TODO: expose the current state in more detail
    if (!state.equals(State.TERMINATED) || !state.equals(State.FAILED)) {
      return Response.ok().build();
    } else {
      return Response.status(Response.Status.GONE).build();
    }
  }
}
