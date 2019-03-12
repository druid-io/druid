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

package org.apache.druid.query.aggregation.tdigestsketch;

import com.tdunning.math.stats.MergingDigest;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.concurrent.GuardedBy;
import java.nio.ByteBuffer;
import java.util.IdentityHashMap;

/**
 * Aggregator that is capable of combining t-digest sketches serialized as {@link ByteBuffer}
 */
public class TDigestMergeSketchBufferAggregator implements BufferAggregator
{
  private final ColumnValueSelector<MergingDigest> selector;
  private final int compression;
  @GuardedBy("this")
  private final IdentityHashMap<ByteBuffer, Int2ObjectMap<MergingDigest>> sketches = new IdentityHashMap<>();

  public TDigestMergeSketchBufferAggregator(
      ColumnValueSelector<MergingDigest> selector,
      int compression
  )
  {
    this.selector = selector;
    this.compression = compression;
  }

  @Override
  public synchronized void init(ByteBuffer buffer, int position)
  {
    MergingDigest emptyDigest = new MergingDigest(compression);
    addToMap(buffer, position, emptyDigest);
  }

  @Override
  public synchronized void aggregate(ByteBuffer buffer, int position)
  {
    final MergingDigest sketch = selector.getObject();
    if (sketch == null) {
      return;
    }
    final MergingDigest union = sketches.get(buffer).get(position);
    union.add(sketch);
  }

  @Override
  public synchronized Object get(ByteBuffer buffer, int position)
  {
    return sketches.get(buffer).get(position);
  }

  @Override
  public float getFloat(final ByteBuffer buffer, final int position)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public long getLong(final ByteBuffer buffer, final int position)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public synchronized void close()
  {
    sketches.clear();
  }

  @Override
  public synchronized void relocate(int oldPosition, int newPosition, ByteBuffer oldBuffer, ByteBuffer newBuffer)
  {
    MergingDigest sketch = sketches.get(oldBuffer).get(oldPosition);
    addToMap(newBuffer, newPosition, sketch);
    final Int2ObjectMap<MergingDigest> map = sketches.get(oldBuffer);
    map.remove(oldPosition);
    if (map.isEmpty()) {
      sketches.remove(oldBuffer);
    }
  }

  private synchronized void addToMap(final ByteBuffer buffer, final int position, final MergingDigest union)
  {
    Int2ObjectMap<MergingDigest> map = sketches.computeIfAbsent(buffer, buf -> new Int2ObjectOpenHashMap<>());
    map.put(position, union);
  }

}
