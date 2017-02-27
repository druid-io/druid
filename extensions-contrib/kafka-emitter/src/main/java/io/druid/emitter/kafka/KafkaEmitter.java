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

package io.druid.emitter.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.metamx.emitter.core.Emitter;
import com.metamx.emitter.core.Event;
import com.metamx.emitter.service.AlertEvent;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.emitter.kafka.MemoryBoundLinkedBlockingQueue.ObjectContainer;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.java.util.common.logger.Logger;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class KafkaEmitter implements Emitter {
  private static Logger log = new Logger(KafkaEmitter.class);

  private final static String DEFAULT_KEY_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
  private final static String DEFAULT_VALUE_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
  private final static int DEFAULT_RETRIES = 3;
  private long queueMemoryBound = 33554432L; // same with default value of kafka producer's buffer.memory
  private final AtomicLong metricLost;
  private final AtomicLong alertLost;
  private final AtomicLong invalidLost;

  private final KafkaEmitterConfig config;
  private final Producer<String, String> producer;
  private final Callback producerCallback;
  private final ObjectMapper jsonMapper;
  private final MemoryBoundLinkedBlockingQueue<String> metricQueue;
  private final MemoryBoundLinkedBlockingQueue<String> alertQueue;
  private final ScheduledExecutorService scheduler;

  public KafkaEmitter(final KafkaEmitterConfig config, ObjectMapper jsonMapper) {
    this.config = config;
    this.jsonMapper = jsonMapper;
    this.producer = getKafkaProducer(config);
    this.producerCallback = new Callback() {
      @Override
      public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        if(e != null) {
          if(recordMetadata.topic().equals(config.getMetricTopic())) {
            metricLost.incrementAndGet();
          } else if (recordMetadata.topic().equals(config.getAlertTopic())) {
            alertLost.incrementAndGet();
          } else {
            invalidLost.incrementAndGet();
          }
        }
      }
    };
    this.metricQueue = new MemoryBoundLinkedBlockingQueue<>(queueMemoryBound);
    this.alertQueue = new MemoryBoundLinkedBlockingQueue<>(queueMemoryBound);
    this.scheduler = Executors.newScheduledThreadPool(3);
    this.metricLost = new AtomicLong(0L);
    this.alertLost = new AtomicLong(0L);
    this.invalidLost = new AtomicLong(0L);
  }

  private Producer<String, String> getKafkaProducer(KafkaEmitterConfig config) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, DEFAULT_KEY_SERIALIZER);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, DEFAULT_VALUE_SERIALIZER);
    props.put(ProducerConfig.RETRIES_CONFIG, DEFAULT_RETRIES);
    props.putAll(config.getKafkaProducerConfig());
    queueMemoryBound = (props.containsKey(ProducerConfig.BUFFER_MEMORY_CONFIG) ?
        Long.parseLong(props.getProperty(ProducerConfig.BUFFER_MEMORY_CONFIG)) : queueMemoryBound);

    return new KafkaProducer<>(props);
  }

  @Override
  @LifecycleStart
  public void start() {
    scheduler.scheduleWithFixedDelay(new Runnable() {
      public void run() {
        sendMetricToKafka();
      }
    }, 10, 10, TimeUnit.SECONDS);
    scheduler.scheduleWithFixedDelay(new Runnable() {
      public void run() {
        sendAlertToKafka();
      }
    }, 10, 10, TimeUnit.SECONDS);
    scheduler.scheduleWithFixedDelay(new Runnable() {
      @Override
      public void run() {
        log.info("Message lost counter: metricLost=[%d] / alertLost=[%d] / invalidLost=[%d]",
                 metricLost.get(), alertLost.get(), invalidLost.get());
      }
    }, 5, 5, TimeUnit.MINUTES);
    log.info("Starting Kafka Emitter.");
  }

  private void sendMetricToKafka() {
    sendToKafka(config.getMetricTopic(), metricQueue);
  }

  private void sendAlertToKafka() {
    sendToKafka(config.getAlertTopic(), alertQueue);
  }

  private void sendToKafka(final String topic, MemoryBoundLinkedBlockingQueue<String> recordQueue) {
    ObjectContainer<String> objectToSend;
    try {
      while((objectToSend = recordQueue.take()) != null) {
        producer.send(new ProducerRecord<String, String>(topic, objectToSend.getData()), producerCallback);
      }
    } catch (InterruptedException e) {
      log.warn(e, "Failed to take record from queue!");
    }
  }

  @Override
  public void emit(final Event event) {
    if(event != null) {
      ImmutableMap.Builder<String, Object> resultBuilder = ImmutableMap.<String, Object>builder().putAll(event.toMap());
      if (config.getClusterName() != null) {
        resultBuilder.put("clusterName", config.getClusterName());
      }
      Map<String, Object> result = resultBuilder.build();

      try {
        String resultJson = jsonMapper.writeValueAsString(result);
        ObjectContainer<String> objectContainer = new ObjectContainer<>(resultJson, resultJson.getBytes().length);
        if(event instanceof ServiceMetricEvent) {
          if(!metricQueue.offer(objectContainer)) {
            metricLost.incrementAndGet();
          }
        } else if(event instanceof AlertEvent) {
          if(!alertQueue.offer(objectContainer)) {
            alertLost.incrementAndGet();
          }
        } else {
          invalidLost.incrementAndGet();
        }
      } catch (JsonProcessingException e) {
        invalidLost.incrementAndGet();
      }
    }
  }

  @Override
  public void flush() throws IOException {
    producer.flush();
  }

  @Override
  @LifecycleStop
  public void close() throws IOException {
    scheduler.shutdownNow();
    producer.close();
  }
}
