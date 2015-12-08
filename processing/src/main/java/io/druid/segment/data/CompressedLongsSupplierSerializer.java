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

package io.druid.segment.data;

import com.google.common.primitives.Ints;
import io.druid.collections.ResourceHolder;
import io.druid.collections.StupidResourceHolder;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteOrder;
import java.nio.LongBuffer;

/**
 */
public class CompressedLongsSupplierSerializer
{
  public static CompressedLongsSupplierSerializer create(
      IOPeon ioPeon, final String filenameBase, final ByteOrder order, final CompressedObjectStrategy.CompressionStrategy compression
  ) throws IOException
  {
    final CompressedLongsSupplierSerializer retVal = new CompressedLongsSupplierSerializer(
        CompressedLongsIndexedSupplier.MAX_LONGS_IN_BUFFER,
        new GenericIndexedWriter<ResourceHolder<LongBuffer>>(
            ioPeon, filenameBase, CompressedLongBufferObjectStrategy.getBufferForOrder(order, compression, CompressedLongsIndexedSupplier.MAX_LONGS_IN_BUFFER)
        ),
        compression
    );
    return retVal;
  }

  private final int sizePer;
  private final GenericIndexedWriter<ResourceHolder<LongBuffer>> flattener;
  private final CompressedObjectStrategy.CompressionStrategy compression;

  private int numInserted = 0;

  private LongBuffer endBuffer;

  public CompressedLongsSupplierSerializer(
      int sizePer,
      GenericIndexedWriter<ResourceHolder<LongBuffer>> flattener,
      CompressedObjectStrategy.CompressionStrategy compression
  )
  {
    this.sizePer = sizePer;
    this.flattener = flattener;
    this.compression = compression;

    endBuffer = LongBuffer.allocate(sizePer);
    endBuffer.mark();
  }

  public void open() throws IOException
  {
    flattener.open();
  }

  public int size()
  {
    return numInserted;
  }

  public void add(long value) throws IOException
  {
    if (! endBuffer.hasRemaining()) {
      endBuffer.rewind();
      flattener.write(StupidResourceHolder.create(endBuffer));
      endBuffer = LongBuffer.allocate(sizePer);
      endBuffer.mark();
    }

    endBuffer.put(value);
    ++numInserted;
  }

  public void closeAndConsolidate(OutputStream consolidate) throws IOException
  {
    endBuffer.limit(endBuffer.position());
    endBuffer.rewind();
    flattener.write(StupidResourceHolder.create(endBuffer));
    endBuffer = null;
    
    flattener.close();

    try (OutputStream out = consolidate) {
      out.write(CompressedLongsIndexedSupplier.version);
      out.write(Ints.toByteArray(numInserted));
      out.write(Ints.toByteArray(sizePer));
      out.write(new byte[]{compression.getId()});
      flattener.combineStreams().copyTo(out);
    }
  }
}
