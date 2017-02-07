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

package io.druid.query.cache;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import io.druid.common.utils.StringUtils;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class CacheKeyBuilder
{
  static final byte BYTE_KEY = 0;
  static final byte BYTE_ARRAY_KEY = 1;
  static final byte BOOLEAN_KEY = 2;
  static final byte INT_KEY = 3;
  static final byte FLOAT_KEY = 4;
  static final byte FLOAT_ARRAY_KEY = 5;
  static final byte DOUBLE_KEY = 6;
  static final byte STRING_KEY = 7;
  static final byte STRING_LIST_KEY = 8;
  static final byte CACHEABLE_KEY = 9;
  static final byte CACHEABLE_LIST_KEY = 10;

  static final byte[] STRING_SEPARATOR = new byte[]{(byte) 0xFF};
  public static final byte[] EMPTY_BYTES = StringUtils.EMPTY_BYTES;

  private static class Item
  {
    private final byte typeKey;
    private final byte[] item;

    Item(byte typeKey, byte[] item)
    {
      this.typeKey = typeKey;
      this.item = item;
    }

    public int byteSize()
    {
      return 1 + item.length;
    }
  }

  private static byte[] stringListToByteArray(List<String> input)
  {
    if (input.size() > 0) {
      List<byte[]> byteArrayList = Lists.newArrayListWithCapacity(input.size());
      int totalByteLength = 0;
      for (String eachStr : input) {
        final byte[] byteArray = StringUtils.toUtf8WithNullToEmpty(eachStr);
        totalByteLength += byteArray.length;
        byteArrayList.add(byteArray);
      }
      return joinByteArrayList(byteArrayList, STRING_SEPARATOR, totalByteLength);
    } else {
      return EMPTY_BYTES;
    }
  }

  private static byte[] joinByteArrayList(Collection<byte[]> byteArrayList, byte[] separator, int totalByteLength)
  {
    final Iterator<byte[]> iterator = byteArrayList.iterator();
    Preconditions.checkArgument(iterator.hasNext());

    final int bufSize = Ints.BYTES + separator.length * (byteArrayList.size() - 1) + totalByteLength;
    final ByteBuffer buffer = ByteBuffer.allocate(bufSize)
                                        .putInt(byteArrayList.size())
                                        .put(iterator.next());

    while (iterator.hasNext()) {
      buffer.put(separator).put(iterator.next());
    }

    return buffer.array();
  }

  private static byte[] floatArrayToByteArray(float[] input)
  {
    final ByteBuffer buffer = ByteBuffer.allocate(Floats.BYTES * input.length);
    buffer.asFloatBuffer().put(input);
    return buffer.array();
  }

  private static byte[] cacheableToByteArray(@Nullable Cacheable cacheable)
  {
    if (cacheable == null) {
      return EMPTY_BYTES;
    } else {
      final byte[] key = cacheable.getCacheKey();
      Preconditions.checkArgument(!Arrays.equals(key, EMPTY_BYTES), "cache key is equal to the empty key");
      return key;
    }
  }

  private static byte[] cacheableListToByteArray(List<? extends Cacheable> input)
  {
    final int inputSize = input.size();
    if (inputSize > 0) {
      final List<byte[]> byteArrayList = Lists.newArrayListWithCapacity(inputSize);
      int totalByteLength = 0;
      for (Cacheable eachCacheable : input) {
        byte[] key = cacheableToByteArray(eachCacheable);
        totalByteLength += key.length;
        byteArrayList.add(key);
      }

      return joinByteArrayList(byteArrayList, EMPTY_BYTES, totalByteLength);
    } else {
      return EMPTY_BYTES;
    }
  }

  private final List<Item> items = Lists.newArrayList();
  private final byte id;
  private int size;

  public CacheKeyBuilder(byte id)
  {
    this.id = id;
    this.size = 1;
  }

  public CacheKeyBuilder appendByte(byte input)
  {
    appendItem(BYTE_KEY, new byte[]{input});
    return this;
  }

  public CacheKeyBuilder appendByteArray(byte[] input)
  {
    // TODO: check it is ok to not add input.length for fixed-size types
    appendItem(BYTE_ARRAY_KEY, input);
    return this;
  }

  public CacheKeyBuilder appendString(@Nullable String input)
  {
    appendItem(STRING_KEY, StringUtils.toUtf8WithNullToEmpty(input));
    return this;
  }

  public CacheKeyBuilder appendStringList(List<String> input)
  {
    appendItem(STRING_LIST_KEY, stringListToByteArray(input));
    return this;
  }

  public CacheKeyBuilder appendBoolean(boolean input)
  {
    appendItem(BOOLEAN_KEY, new byte[]{(byte) (input ? 1 : 0)});
    return this;
  }

  public CacheKeyBuilder appendInt(int input)
  {
    appendItem(INT_KEY, Ints.toByteArray(input));
    return this;
  }

  public CacheKeyBuilder appendFloat(float input)
  {
    appendItem(FLOAT_KEY, ByteBuffer.allocate(Floats.BYTES).putFloat(input).array());
    return this;
  }

  public CacheKeyBuilder appendDouble(double input)
  {
    appendItem(DOUBLE_KEY, ByteBuffer.allocate(Doubles.BYTES).putDouble(input).array());
    return this;
  }

  public CacheKeyBuilder appendFloatArray(float[] input)
  {
    appendItem(FLOAT_ARRAY_KEY, floatArrayToByteArray(input));
    return this;
  }

  public CacheKeyBuilder appendCacheable(@Nullable Cacheable input)
  {
    appendItem(CACHEABLE_KEY, cacheableToByteArray(input));
    return this;
  }

  public CacheKeyBuilder appendCacheableList(List<? extends Cacheable> input)
  {
    appendItem(CACHEABLE_LIST_KEY, cacheableListToByteArray(input));
    return this;
  }

  private void appendItem(byte typeKey, byte[] input)
  {
    final Item item = new Item(typeKey, input);
    items.add(item);
    size += item.byteSize();
  }

  public byte[] build()
  {
    final ByteBuffer buffer = ByteBuffer.allocate(size);
    buffer.put(id);

    for (Item item : items) {
      buffer.put(item.typeKey).put(item.item);
    }

    return buffer.array();
  }
}
