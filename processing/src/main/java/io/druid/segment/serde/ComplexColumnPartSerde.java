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

package io.druid.segment.serde;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.druid.java.util.common.io.smoosh.FileSmoosher;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.column.ColumnConfig;
import io.druid.segment.data.GenericIndexed;

/**
 */
public class ComplexColumnPartSerde implements ColumnPartSerde
{
  @JsonCreator
  public static ComplexColumnPartSerde createDeserializer(
      @JsonProperty("typeName") String complexType
  )
  {
    return new ComplexColumnPartSerde(complexType, null);
  }

  private final String typeName;
  private final ComplexMetricSerde serde;
  private final Serializer serializer;

  private ComplexColumnPartSerde(String typeName, Serializer serializer)
  {
    this(typeName, ComplexMetrics.getSerdeForType(typeName), serializer);
  }

  private ComplexColumnPartSerde(String typeName, ComplexMetricSerde serde, Serializer serializer)
  {
    this.typeName = typeName;
    this.serde = serde;
    this.serializer = serializer;
  }

  @JsonProperty
  public String getTypeName()
  {
    return typeName;
  }

  public static SerializerBuilder serializerBuilder()
  {
    return new SerializerBuilder();
  }

  public static class SerializerBuilder
  {
    private String typeName = null;
    private ComplexColumnSerializer delegate = null;

    public SerializerBuilder withTypeName(final String typeName)
    {
      this.typeName = typeName;
      return this;
    }

    public SerializerBuilder withDelegate(final ComplexColumnSerializer delegate)
    {
      this.delegate = delegate;
      return this;
    }

    public ComplexColumnPartSerde build()
    {
      return new ComplexColumnPartSerde(
          typeName, new Serializer()
      {
        @Override
        public long numBytes()
        {
          return delegate.getSerializedSize();
        }

        @Override
        public void write(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
        {
          delegate.writeToChannel(channel, smoosher);
        }
      }
      );
    }
  }

  public static LegacySerializerBuilder legacySerializerBuilder()
  {
    return new LegacySerializerBuilder();
  }

  public static class LegacySerializerBuilder
  {
    private String typeName = null;
    private GenericIndexed delegate = null;

    public LegacySerializerBuilder withTypeName(final String typeName)
    {
      this.typeName = typeName;
      return this;
    }

    public LegacySerializerBuilder withDelegate(final GenericIndexed delegate)
    {
      this.delegate = delegate;
      return this;
    }

    public ComplexColumnPartSerde build()
    {
      return new ComplexColumnPartSerde(
          typeName, new Serializer()
      {
        @Override
        public long numBytes()
        {
          return delegate.getSerializedSize();
        }

        @Override
            public void write(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
        {
          delegate.writeToChannel(channel);
        }
      }
      );
    }
  }

  @Override
  public Serializer getSerializer()
  {
    return serializer;
  }

  @Override
  public Deserializer getDeserializer()
  {
    return new Deserializer()
    {
      @Override
      public void read(ByteBuffer buffer, ColumnBuilder builder, ColumnConfig columnConfig)
      {
        if (serde != null) {
          serde.deserializeColumn(buffer, builder);
        }
      }
    };
  }
}
