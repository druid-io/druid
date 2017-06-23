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

package io.druid.query;

import io.druid.java.util.common.StringUtils;

/**
 * Exception indicating that an operation failed because it exceeded some configured resource limit.
 *
 * This is used as a marker exception by {@link QueryInterruptedException} to report the "Resource limit exceeded"
 * error code.
 */
public class ResourceLimitExceededException extends RuntimeException
{
  public ResourceLimitExceededException(String message, Object... arguments)
  {
    super(StringUtils.unimportantSafeFormat(message, arguments));
  }
}
