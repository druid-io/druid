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

package io.druid.query.aggregation;

import io.druid.segment.FloatColumnSelector;

import java.nio.ByteBuffer;

/**
 */
public class FloatSumBufferAggregator implements BufferAggregator
{
  private final FloatColumnSelector selector;

  public FloatSumBufferAggregator(
      FloatColumnSelector selector
  )
  {
    this.selector = selector;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.putFloat(position, 0.0f);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    buf.putFloat(position, buf.getFloat(position) + selector.get());
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return buf.getFloat(position);
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    return buf.getFloat(position);
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    return (long) buf.getFloat(position);
  }

  @Override
  public int getInt(ByteBuffer buf, int position)
  {
    return (int) buf.getFloat(position);
  }

  @Override
  public double getDouble(ByteBuffer buf, int position)
  {
    return (double) buf.getFloat(position);
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
