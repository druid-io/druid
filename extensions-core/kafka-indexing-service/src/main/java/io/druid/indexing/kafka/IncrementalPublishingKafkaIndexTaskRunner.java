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
package io.druid.indexing.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.druid.data.input.Committer;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.InputRowParser;
import io.druid.discovery.DiscoveryDruidNode;
import io.druid.discovery.DruidNodeDiscoveryProvider;
import io.druid.discovery.LookupNodeService;
import io.druid.indexer.IngestionState;
import io.druid.indexing.common.IngestionStatsAndErrorsTaskReport;
import io.druid.indexing.common.IngestionStatsAndErrorsTaskReportData;
import io.druid.indexing.common.TaskRealtimeMetricsMonitorBuilder;
import io.druid.indexing.common.TaskReport;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.CheckPointDataSourceMetadataAction;
import io.druid.indexing.common.actions.ResetDataSourceMetadataAction;
import io.druid.indexing.common.actions.SegmentTransactionalInsertAction;
import io.druid.indexing.common.stats.RowIngestionMeters;
import io.druid.indexing.common.stats.RowIngestionMetersFactory;
import io.druid.indexing.common.task.IndexTaskUtils;
import io.druid.indexing.common.task.RealtimeIndexTask;
import io.druid.indexing.kafka.KafkaIndexTask.Status;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.collect.Utils;
import io.druid.java.util.common.concurrent.Execs;
import io.druid.java.util.common.parsers.ParseException;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.segment.indexing.RealtimeIOConfig;
import io.druid.segment.realtime.FireDepartment;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.appenderator.Appenderator;
import io.druid.segment.realtime.appenderator.AppenderatorDriverAddResult;
import io.druid.segment.realtime.appenderator.SegmentsAndMetadata;
import io.druid.segment.realtime.appenderator.StreamAppenderatorDriver;
import io.druid.segment.realtime.appenderator.TransactionalSegmentPublisher;
import io.druid.segment.realtime.firehose.ChatHandlerProvider;
import io.druid.server.security.Access;
import io.druid.server.security.Action;
import io.druid.server.security.AuthorizerMapper;
import io.druid.timeline.DataSegment;
import io.druid.utils.CircularBuffer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.TopicPartition;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * Kafka indexing task runner supporting incremental segments publishing
 */
public class IncrementalPublishingKafkaIndexTaskRunner implements KafkaIndexTaskRunner
{
  private static final EmittingLogger log = new EmittingLogger(IncrementalPublishingKafkaIndexTaskRunner.class);
  private static final String METADATA_NEXT_PARTITIONS = "nextPartitions";
  private static final String METADATA_PUBLISH_PARTITIONS = "publishPartitions";

  private final Map<Integer, Long> endOffsets = new ConcurrentHashMap<>();
  private final Map<Integer, Long> nextOffsets = new ConcurrentHashMap<>();
  private final Map<Integer, Long> lastPersistedOffsets = new ConcurrentHashMap<>();

  // The pause lock and associated conditions are to support coordination between the Jetty threads and the main
  // ingestion loop. The goal is to provide callers of the API a guarantee that if pause() returns successfully
  // the ingestion loop has been stopped at the returned offsets and will not ingest any more data until resumed. The
  // fields are used as follows (every step requires acquiring [pauseLock]):
  //   Pausing:
  //   - In pause(), [pauseRequested] is set to true and then execution waits for [status] to change to PAUSED, with the
  //     condition checked when [hasPaused] is signalled.
  //   - In possiblyPause() called from the main loop, if [pauseRequested] is true, [status] is set to PAUSED,
  //     [hasPaused] is signalled, and execution pauses until [pauseRequested] becomes false, either by being set or by
  //     the [pauseMillis] timeout elapsing. [pauseRequested] is checked when [shouldResume] is signalled.
  //   Resuming:
  //   - In resume(), [pauseRequested] is set to false, [shouldResume] is signalled, and execution waits for [status] to
  //     change to something other than PAUSED, with the condition checked when [shouldResume] is signalled.
  //   - In possiblyPause(), when [shouldResume] is signalled, if [pauseRequested] has become false the pause loop ends,
  //     [status] is changed to STARTING and [shouldResume] is signalled.

  private final Lock pauseLock = new ReentrantLock();
  private final Condition hasPaused = pauseLock.newCondition();
  private final Condition shouldResume = pauseLock.newCondition();

  private final AtomicBoolean stopRequested = new AtomicBoolean(false);
  private final AtomicBoolean publishOnStop = new AtomicBoolean(false);

  // [statusLock] is used to synchronize the Jetty thread calling stopGracefully() with the main run thread. It prevents
  // the main run thread from switching into a publishing state while the stopGracefully() thread thinks it's still in
  // a pre-publishing state. This is important because stopGracefully() will try to use the [stopRequested] flag to stop
  // the main thread where possible, but this flag is not honored once publishing has begun so in this case we must
  // interrupt the thread. The lock ensures that if the run thread is about to transition into publishing state, it
  // blocks until after stopGracefully() has set [stopRequested] and then does a final check on [stopRequested] before
  // transitioning to publishing state.
  private final Object statusLock = new Object();

  private final Lock pollRetryLock = new ReentrantLock();
  private final Condition isAwaitingRetry = pollRetryLock.newCondition();

  private final KafkaIndexTask task;
  private final KafkaIOConfig ioConfig;
  private final KafkaTuningConfig tuningConfig;
  private final InputRowParser<ByteBuffer> parser;
  private final AuthorizerMapper authorizerMapper;
  private final Optional<ChatHandlerProvider> chatHandlerProvider;
  private final CircularBuffer<Throwable> savedParseExceptions;
  private final String topic;
  private final RowIngestionMeters rowIngestionMeters;

  private final Set<String> publishingSequences = Sets.newConcurrentHashSet();
  private final BlockingQueue<SequenceMetadata> publishQueue = new LinkedBlockingQueue<>();
  private final List<ListenableFuture<SegmentsAndMetadata>> handOffWaitList = new CopyOnWriteArrayList<>(); // to prevent concurrency visibility issue
  private final CountDownLatch waitForPublishes = new CountDownLatch(1);
  private final AtomicReference<Throwable> throwableAtomicReference = new AtomicReference<>();

  private volatile DateTime startTime;
  private volatile Status status = Status.NOT_STARTED; // this is only ever set by the task runner thread (runThread)
  private volatile TaskToolbox toolbox;
  private volatile Thread runThread;
  private volatile Appenderator appenderator;
  private volatile StreamAppenderatorDriver driver;
  private volatile FireDepartmentMetrics fireDepartmentMetrics;
  private volatile IngestionState ingestionState;

  private volatile boolean pauseRequested = false;
  private volatile long pauseMillis = 0;
  private volatile long nextCheckpointTime;

  private volatile CopyOnWriteArrayList<SequenceMetadata> sequences;
  private volatile File sequencesPersistFile;
  private ListeningExecutorService publishExecService;

  public IncrementalPublishingKafkaIndexTaskRunner(
      KafkaIndexTask task,
      InputRowParser<ByteBuffer> parser,
      AuthorizerMapper authorizerMapper,
      Optional<ChatHandlerProvider> chatHandlerProvider,
      CircularBuffer<Throwable> savedParseExceptions,
      RowIngestionMetersFactory rowIngestionMetersFactory
  )
  {
    this.task = task;
    this.ioConfig = task.getIOConfig();
    this.tuningConfig = task.getTuningConfig();
    this.parser = parser;
    this.authorizerMapper = authorizerMapper;
    this.chatHandlerProvider = chatHandlerProvider;
    this.savedParseExceptions = savedParseExceptions;
    this.topic = ioConfig.getStartPartitions().getTopic();
    this.rowIngestionMeters = rowIngestionMetersFactory.createRowIngestionMeters();

    this.endOffsets.putAll(ioConfig.getEndPartitions().getPartitionOffsetMap());
    this.sequences = new CopyOnWriteArrayList<>();
    this.ingestionState = IngestionState.NOT_STARTED;

    resetNextCheckpointTime();
  }

  @Override
  public TaskStatus run(TaskToolbox toolbox)
  {
    try {
      return runInternal(toolbox);
    }
    catch (Exception e) {
      log.error(e, "Encountered exception while running task.");
      final String errorMsg = Throwables.getStackTraceAsString(e);
      toolbox.getTaskReportFileWriter().write(getTaskCompletionReports(errorMsg));
      return TaskStatus.failure(
          task.getId(),
          errorMsg
      );
    }
  }

  private TaskStatus runInternal(TaskToolbox toolbox) throws Exception
  {
    log.info("Starting up!");

    startTime = DateTimes.nowUtc();
    status = Status.STARTING;
    this.toolbox = toolbox;

    final Map<String, Object> context = task.getContext();
    if (context != null && context.get("checkpoints") != null) {
      final String checkpointsString = (String) context.get("checkpoints");
      log.info("Got checkpoints [%s]", checkpointsString);
      final TreeMap<Integer, Map<Integer, Long>> checkpoints = toolbox.getObjectMapper().readValue(
          checkpointsString,
          new TypeReference<TreeMap<Integer, Map<Integer, Long>>>()
          {
          }
      );

      Iterator<Entry<Integer, Map<Integer, Long>>> sequenceOffsets = checkpoints.entrySet().iterator();
      Map.Entry<Integer, Map<Integer, Long>> previous = sequenceOffsets.next();
      while (sequenceOffsets.hasNext()) {
        Map.Entry<Integer, Map<Integer, Long>> current = sequenceOffsets.next();
        sequences.add(new SequenceMetadata(
            previous.getKey(),
            StringUtils.format("%s_%s", ioConfig.getBaseSequenceName(), previous.getKey()),
            previous.getValue(),
            current.getValue(),
            true
        ));
        previous = current;
      }
      sequences.add(new SequenceMetadata(
          previous.getKey(),
          StringUtils.format("%s_%s", ioConfig.getBaseSequenceName(), previous.getKey()),
          previous.getValue(),
          endOffsets,
          false
      ));
    } else {
      sequences.add(new SequenceMetadata(
          0,
          StringUtils.format("%s_%s", ioConfig.getBaseSequenceName(), 0),
          ioConfig.getStartPartitions().getPartitionOffsetMap(),
          endOffsets,
          false
      ));
    }

    sequencesPersistFile = new File(toolbox.getPersistDir(), "sequences.json");
    restoreSequences();
    log.info("Starting with sequences:  %s", sequences);

    if (chatHandlerProvider.isPresent()) {
      log.info("Found chat handler of class[%s]", chatHandlerProvider.get().getClass().getName());
      chatHandlerProvider.get().register(task.getId(), this, false);
    } else {
      log.warn("No chat handler detected");
    }

    runThread = Thread.currentThread();

    // Set up FireDepartmentMetrics
    final FireDepartment fireDepartmentForMetrics = new FireDepartment(
        task.getDataSchema(),
        new RealtimeIOConfig(null, null, null),
        null
    );
    fireDepartmentMetrics = fireDepartmentForMetrics.getMetrics();
    toolbox.getMonitorScheduler()
           .addMonitor(TaskRealtimeMetricsMonitorBuilder.build(task, fireDepartmentForMetrics, rowIngestionMeters));

    final String lookupTier = task.getContextValue(RealtimeIndexTask.CTX_KEY_LOOKUP_TIER);
    LookupNodeService lookupNodeService = lookupTier == null ?
                                          toolbox.getLookupNodeService() :
                                          new LookupNodeService(lookupTier);
    DiscoveryDruidNode discoveryDruidNode = new DiscoveryDruidNode(
        toolbox.getDruidNode(),
        DruidNodeDiscoveryProvider.NODE_TYPE_PEON,
        ImmutableMap.of(
            toolbox.getDataNodeService().getName(), toolbox.getDataNodeService(),
            lookupNodeService.getName(), lookupNodeService
        )
    );

    try (
        final KafkaConsumer<byte[], byte[]> consumer = task.newConsumer()
    ) {
      toolbox.getDataSegmentServerAnnouncer().announce();
      toolbox.getDruidNodeAnnouncer().announce(discoveryDruidNode);

      appenderator = task.newAppenderator(fireDepartmentMetrics, toolbox);
      driver = task.newDriver(appenderator, toolbox, fireDepartmentMetrics);
      createAndStartPublishExecutor();

      final String topic = ioConfig.getStartPartitions().getTopic();

      // Start up, set up initial offsets.
      final Object restoredMetadata = driver.startJob();
      if (restoredMetadata == null) {
        // no persist has happened so far
        // so either this is a brand new task or replacement of a failed task
        Preconditions.checkState(sequences.get(0).startOffsets.entrySet().stream().allMatch(
            partitionOffsetEntry -> Longs.compare(
                partitionOffsetEntry.getValue(),
                ioConfig.getStartPartitions()
                        .getPartitionOffsetMap()
                        .get(partitionOffsetEntry.getKey())
            ) >= 0
        ), "Sequence offsets are not compatible with start offsets of task");
        nextOffsets.putAll(sequences.get(0).startOffsets);
      } else {
        final Map<String, Object> restoredMetadataMap = (Map) restoredMetadata;
        final KafkaPartitions restoredNextPartitions = toolbox.getObjectMapper().convertValue(
            restoredMetadataMap.get(METADATA_NEXT_PARTITIONS),
            KafkaPartitions.class
        );
        nextOffsets.putAll(restoredNextPartitions.getPartitionOffsetMap());

        // Sanity checks.
        if (!restoredNextPartitions.getTopic().equals(ioConfig.getStartPartitions().getTopic())) {
          throw new ISE(
              "WTF?! Restored topic[%s] but expected topic[%s]",
              restoredNextPartitions.getTopic(),
              ioConfig.getStartPartitions().getTopic()
          );
        }

        if (!nextOffsets.keySet().equals(ioConfig.getStartPartitions().getPartitionOffsetMap().keySet())) {
          throw new ISE(
              "WTF?! Restored partitions[%s] but expected partitions[%s]",
              nextOffsets.keySet(),
              ioConfig.getStartPartitions().getPartitionOffsetMap().keySet()
          );
        }
        // sequences size can be 0 only when all sequences got published and task stopped before it could finish
        // which is super rare
        if (sequences.size() == 0 || sequences.get(sequences.size() - 1).isCheckpointed()) {
          this.endOffsets.putAll(sequences.size() == 0
                                 ? nextOffsets
                                 : sequences.get(sequences.size() - 1).getEndOffsets());
          log.info("End offsets changed to [%s]", endOffsets);
        }
      }

      // Set up committer.
      final Supplier<Committer> committerSupplier = () -> {
        final Map<Integer, Long> snapshot = ImmutableMap.copyOf(nextOffsets);
        lastPersistedOffsets.clear();
        lastPersistedOffsets.putAll(snapshot);

        return new Committer()
        {
          @Override
          public Object getMetadata()
          {
            return ImmutableMap.of(
                METADATA_NEXT_PARTITIONS, new KafkaPartitions(
                    ioConfig.getStartPartitions().getTopic(),
                    snapshot
                )
            );
          }

          @Override
          public void run()
          {
            // Do nothing.
          }
        };
      };

      // restart publishing of sequences (if any)
      maybePersistAndPublishSequences(committerSupplier);

      Set<Integer> assignment = assignPartitionsAndSeekToNext(consumer, topic);

      ingestionState = IngestionState.BUILD_SEGMENTS;

      // Main loop.
      // Could eventually support leader/follower mode (for keeping replicas more in sync)
      boolean stillReading = !assignment.isEmpty();
      status = Status.READING;
      try {
        while (stillReading) {
          if (possiblyPause(assignment)) {
            // The partition assignments may have changed while paused by a call to setEndOffsets() so reassign
            // partitions upon resuming. This is safe even if the end offsets have not been modified.
            assignment = assignPartitionsAndSeekToNext(consumer, topic);

            if (assignment.isEmpty()) {
              log.info("All partitions have been fully read");
              publishOnStop.set(true);
              stopRequested.set(true);
            }
          }

          // if stop is requested or task's end offset is set by call to setEndOffsets method with finish set to true
          if (stopRequested.get() || (sequences.get(sequences.size() - 1).isCheckpointed()
                                      && !ioConfig.isPauseAfterRead())) {
            status = Status.PUBLISHING;
          }

          if (stopRequested.get()) {
            break;
          }

          checkAndMaybeThrowException();

          if (!ioConfig.isPauseAfterRead()) {
            maybePersistAndPublishSequences(committerSupplier);
          }

          // The retrying business is because the KafkaConsumer throws OffsetOutOfRangeException if the seeked-to
          // offset is not present in the topic-partition. This can happen if we're asking a task to read from data
          // that has not been written yet (which is totally legitimate). So let's wait for it to show up.
          ConsumerRecords<byte[], byte[]> records = ConsumerRecords.empty();
          try {
            records = consumer.poll(KafkaIndexTask.POLL_TIMEOUT);
          }
          catch (OffsetOutOfRangeException e) {
            log.warn("OffsetOutOfRangeException with message [%s]", e.getMessage());
            possiblyResetOffsetsOrWait(e.offsetOutOfRangePartitions(), consumer, toolbox);
            stillReading = ioConfig.isPauseAfterRead() || !assignment.isEmpty();
          }

          SequenceMetadata sequenceToCheckpoint = null;
          for (ConsumerRecord<byte[], byte[]> record : records) {
            if (log.isTraceEnabled()) {
              log.trace(
                  "Got topic[%s] partition[%d] offset[%,d].",
                  record.topic(),
                  record.partition(),
                  record.offset()
              );
            }

            if (record.offset() < endOffsets.get(record.partition())) {
              if (record.offset() != nextOffsets.get(record.partition())) {
                if (ioConfig.isSkipOffsetGaps()) {
                  log.warn(
                      "Skipped to offset[%,d] after offset[%,d] in partition[%d].",
                      record.offset(),
                      nextOffsets.get(record.partition()),
                      record.partition()
                  );
                } else {
                  throw new ISE(
                      "WTF?! Got offset[%,d] after offset[%,d] in partition[%d].",
                      record.offset(),
                      nextOffsets.get(record.partition()),
                      record.partition()
                  );
                }
              }

              try {
                final byte[] valueBytes = record.value();
                final List<InputRow> rows = valueBytes == null
                                            ? Utils.nullableListOf((InputRow) null)
                                            : parser.parseBatch(ByteBuffer.wrap(valueBytes));
                boolean isPersistRequired = false;

                for (InputRow row : rows) {
                  if (row != null && task.withinMinMaxRecordTime(row)) {
                    SequenceMetadata sequenceToUse = null;
                    for (SequenceMetadata sequence : sequences) {
                      if (sequence.canHandle(record)) {
                        sequenceToUse = sequence;
                      }
                    }

                    if (sequenceToUse == null) {
                      throw new ISE(
                          "WTH?! cannot find any valid sequence for record with partition [%d] and offset [%d]. Current sequences: %s",
                          record.partition(),
                          record.offset(),
                          sequences
                      );
                    }

                    final AppenderatorDriverAddResult addResult = driver.add(
                        row,
                        sequenceToUse.getSequenceName(),
                        committerSupplier,
                        // skip segment lineage check as there will always be one segment
                        // for combination of sequence and segment granularity.
                        // It is necessary to skip it as the task puts messages polled from all the
                        // assigned Kafka partitions into a single Druid segment, thus ordering of
                        // messages among replica tasks across assigned partitions is not guaranteed
                        // which may cause replica tasks to ask for segments with different interval
                        // in different order which might cause SegmentAllocateAction to fail.
                        true,
                        // do not allow incremental persists to happen until all the rows from this batch
                        // of rows are indexed
                        false
                    );

                    if (addResult.isOk()) {
                      // If the number of rows in the segment exceeds the threshold after adding a row,
                      // move the segment out from the active segments of BaseAppenderatorDriver to make a new segment.
                      if (addResult.getNumRowsInSegment() > tuningConfig.getMaxRowsPerSegment()) {
                        if (!sequenceToUse.isCheckpointed()) {
                          sequenceToCheckpoint = sequenceToUse;
                        }
                      }
                      isPersistRequired |= addResult.isPersistRequired();
                    } else {
                      // Failure to allocate segment puts determinism at risk, bail out to be safe.
                      // May want configurable behavior here at some point.
                      // If we allow continuing, then consider blacklisting the interval for a while to avoid constant checks.
                      throw new ISE("Could not allocate segment for row with timestamp[%s]", row.getTimestamp());
                    }

                    if (addResult.getParseException() != null) {
                      handleParseException(addResult.getParseException(), record);
                    } else {
                      rowIngestionMeters.incrementProcessed();
                    }
                  } else {
                    rowIngestionMeters.incrementThrownAway();
                  }
                }
                if (isPersistRequired) {
                  Futures.addCallback(
                      driver.persistAsync(committerSupplier.get()),
                      new FutureCallback<Object>()
                      {
                        @Override
                        public void onSuccess(@Nullable Object result)
                        {
                          log.info("Persist completed with metadata [%s]", result);
                        }

                        @Override
                        public void onFailure(Throwable t)
                        {
                          log.error("Persist failed, dying");
                          throwableAtomicReference.set(t);
                        }
                      }
                  );
                }
              }
              catch (ParseException e) {
                handleParseException(e, record);
              }

              nextOffsets.put(record.partition(), record.offset() + 1);
            }

            if (nextOffsets.get(record.partition()).equals(endOffsets.get(record.partition()))
                && assignment.remove(record.partition())) {
              log.info("Finished reading topic[%s], partition[%,d].", record.topic(), record.partition());
              KafkaIndexTask.assignPartitions(consumer, topic, assignment);
              stillReading = ioConfig.isPauseAfterRead() || !assignment.isEmpty();
            }
          }

          if (System.currentTimeMillis() > nextCheckpointTime) {
            sequenceToCheckpoint = sequences.get(sequences.size() - 1);
          }

          if (sequenceToCheckpoint != null && stillReading) {
            Preconditions.checkArgument(
                sequences.get(sequences.size() - 1)
                         .getSequenceName()
                         .equals(sequenceToCheckpoint.getSequenceName()),
                "Cannot checkpoint a sequence [%s] which is not the latest one, sequences %s",
                sequenceToCheckpoint,
                sequences
            );
            requestPause(KafkaIndexTask.PAUSE_FOREVER);
            if (!toolbox.getTaskActionClient().submit(new CheckPointDataSourceMetadataAction(
                task.getDataSource(),
                ioConfig.getBaseSequenceName(),
                new KafkaDataSourceMetadata(new KafkaPartitions(topic, sequenceToCheckpoint.getStartOffsets())),
                new KafkaDataSourceMetadata(new KafkaPartitions(topic, nextOffsets))
            ))) {
              throw new ISE("Checkpoint request with offsets [%s] failed, dying", nextOffsets);
            }
          }
        }
        ingestionState = IngestionState.COMPLETED;
      }
      catch (Exception e) {
        log.error(e, "Encountered exception in run() before persisting.");
        throw e;
      }
      finally {
        log.info("Persisting all pending data");
        driver.persist(committerSupplier.get()); // persist pending data
      }

      synchronized (statusLock) {
        if (stopRequested.get() && !publishOnStop.get()) {
          throw new InterruptedException("Stopping without publishing");
        }

        status = Status.PUBLISHING;
      }

      for (SequenceMetadata sequenceMetadata : sequences) {
        if (!publishingSequences.contains(sequenceMetadata.getSequenceName())) {
          // this is done to prevent checks in sequence specific commit supplier from failing
          sequenceMetadata.setEndOffsets(nextOffsets);
          sequenceMetadata.updateAssignments(nextOffsets);
          publishingSequences.add(sequenceMetadata.getSequenceName());
          // persist already done in finally, so directly add to publishQueue
          publishQueue.add(sequenceMetadata);
        }
      }

      // add Sentinel SequenceMetadata to indicate end of all sequences
      publishQueue.add(SequenceMetadata.getSentinelSequenceMetadata());
      waitForPublishes.await();
      checkAndMaybeThrowException();

      List<SegmentsAndMetadata> handedOffList = Lists.newArrayList();
      if (tuningConfig.getHandoffConditionTimeout() == 0) {
        handedOffList = Futures.allAsList(handOffWaitList).get();
      } else {
        try {
          handedOffList = Futures.allAsList(handOffWaitList)
                                 .get(tuningConfig.getHandoffConditionTimeout(), TimeUnit.MILLISECONDS);
        }
        catch (TimeoutException e) {
          log.makeAlert("Timed out after [%d] millis waiting for handoffs", tuningConfig.getHandoffConditionTimeout())
             .addData("TaskId", task.getId())
             .emit();
        }
      }

      for (SegmentsAndMetadata handedOff : handedOffList) {
        log.info(
            "Handoff completed for segments[%s] with metadata[%s].",
            Joiner.on(", ").join(
                handedOff.getSegments().stream().map(DataSegment::getIdentifier).collect(Collectors.toList())
            ),
            Preconditions.checkNotNull(handedOff.getCommitMetadata(), "commitMetadata")
        );
      }
    }
    catch (InterruptedException | RejectedExecutionException e) {
      appenderator.closeNow();
      // handle the InterruptedException that gets wrapped in a RejectedExecutionException
      if (e instanceof RejectedExecutionException
          && (e.getCause() == null || !(e.getCause() instanceof InterruptedException))) {
        throw e;
      }

      // if we were interrupted because we were asked to stop, handle the exception and return success, else rethrow
      if (!stopRequested.get()) {
        Thread.currentThread().interrupt();
        throw e;
      }

      log.info("The task was asked to stop before completing");
    }
    finally {
      if (appenderator != null) {
        if (throwableAtomicReference.get() != null) {
          appenderator.closeNow();
        } else {
          appenderator.close();
        }
      }
      if (driver != null) {
        driver.close();
      }
      if (chatHandlerProvider.isPresent()) {
        chatHandlerProvider.get().unregister(task.getId());
      }

      if (publishExecService != null) {
        publishExecService.shutdownNow();
      }

      toolbox.getDruidNodeAnnouncer().unannounce(discoveryDruidNode);
      toolbox.getDataSegmentServerAnnouncer().unannounce();
    }

    toolbox.getTaskReportFileWriter().write(getTaskCompletionReports(null));
    return TaskStatus.success(task.getId());
  }

  private void createAndStartPublishExecutor()
  {
    publishExecService = MoreExecutors.listeningDecorator(Execs.singleThreaded("publish-driver"));
    publishExecService.submit(
        (Runnable) () -> {
          while (true) {
            try {
              final SequenceMetadata sequenceMetadata = publishQueue.take();

              Preconditions.checkNotNull(driver);

              if (sequenceMetadata.isSentinel()) {
                waitForPublishes.countDown();
                break;
              }

              log.info("Publishing segments for sequence [%s]", sequenceMetadata);

              final SegmentsAndMetadata result = driver.publish(
                  sequenceMetadata.getPublisher(toolbox, ioConfig.isUseTransaction()),
                  sequenceMetadata.getCommitterSupplier(topic, lastPersistedOffsets).get(),
                  ImmutableList.of(sequenceMetadata.getSequenceName())
              ).get();

              if (result == null) {
                throw new ISE(
                    "Transaction failure publishing segments for sequence [%s]",
                    sequenceMetadata
                );
              } else {
                log.info(
                    "Published segments[%s] with metadata[%s].",
                    Joiner.on(", ").join(
                        result.getSegments().stream().map(DataSegment::getIdentifier).collect(Collectors.toList())
                    ),
                    Preconditions.checkNotNull(result.getCommitMetadata(), "commitMetadata")
                );
              }

              sequences.remove(sequenceMetadata);
              publishingSequences.remove(sequenceMetadata.getSequenceName());
              try {
                persistSequences();
              }
              catch (IOException e) {
                log.error(e, "Unable to persist state, dying");
                Throwables.propagate(e);
              }

              final ListenableFuture<SegmentsAndMetadata> handOffFuture = driver.registerHandoff(result);
              handOffWaitList.add(handOffFuture);
            }
            catch (Throwable t) {
              if ((t instanceof InterruptedException || (t instanceof RejectedExecutionException
                                                         && t.getCause() instanceof InterruptedException))) {
                log.warn("Stopping publish thread as we are interrupted, probably we are shutting down");
              } else {
                log.makeAlert(t, "Error in publish thread, dying").emit();
                throwableAtomicReference.set(t);
              }
              Futures.allAsList(handOffWaitList).cancel(true);
              waitForPublishes.countDown();
              break;
            }
          }
        }
    );
  }

  private void restoreSequences() throws IOException
  {
    Preconditions.checkNotNull(sequencesPersistFile);
    if (sequencesPersistFile.exists()) {
      sequences = new CopyOnWriteArrayList<>(toolbox.getObjectMapper().<List<SequenceMetadata>>readValue(
          sequencesPersistFile, new TypeReference<List<SequenceMetadata>>()
          {
          }));
    }
  }

  private synchronized void persistSequences() throws IOException
  {
    log.info("Persisting Sequences Metadata [%s]", sequences);
    toolbox.getObjectMapper().writerWithType(
        new TypeReference<List<SequenceMetadata>>()
        {
        }
    ).writeValue(sequencesPersistFile, sequences);
  }

  private Map<String, TaskReport> getTaskCompletionReports(@Nullable String errorMsg)
  {
    return TaskReport.buildTaskReports(
        new IngestionStatsAndErrorsTaskReport(
            task.getId(),
            new IngestionStatsAndErrorsTaskReportData(
                ingestionState,
                getTaskCompletionUnparseableEvents(),
                getTaskCompletionRowStats(),
                errorMsg
            )
        )
    );
  }

  private Map<String, Object> getTaskCompletionUnparseableEvents()
  {
    Map<String, Object> unparseableEventsMap = Maps.newHashMap();
    List<String> buildSegmentsParseExceptionMessages = IndexTaskUtils.getMessagesFromSavedParseExceptions(
        savedParseExceptions
    );
    if (buildSegmentsParseExceptionMessages != null) {
      unparseableEventsMap.put(RowIngestionMeters.BUILD_SEGMENTS, buildSegmentsParseExceptionMessages);
    }
    return unparseableEventsMap;
  }

  private Map<String, Object> getTaskCompletionRowStats()
  {
    Map<String, Object> metrics = Maps.newHashMap();
    metrics.put(
        RowIngestionMeters.BUILD_SEGMENTS,
        rowIngestionMeters.getTotals()
    );
    return metrics;
  }

  private void maybePersistAndPublishSequences(Supplier<Committer> committerSupplier)
      throws InterruptedException
  {
    for (SequenceMetadata sequenceMetadata : sequences) {
      sequenceMetadata.updateAssignments(nextOffsets);
      if (!sequenceMetadata.isOpen() && !publishingSequences.contains(sequenceMetadata.getSequenceName())) {
        publishingSequences.add(sequenceMetadata.getSequenceName());
        try {
          Object result = driver.persist(committerSupplier.get());
          log.info(
              "Persist completed with results: [%s], adding sequence [%s] to publish queue",
              result,
              sequenceMetadata
          );
          publishQueue.add(sequenceMetadata);
        }
        catch (InterruptedException e) {
          log.warn("Interrupted while persisting sequence [%s]", sequenceMetadata);
          throw e;
        }
      }
    }
  }

  private Set<Integer> assignPartitionsAndSeekToNext(KafkaConsumer consumer, String topic)
  {
    // Initialize consumer assignment.
    final Set<Integer> assignment = Sets.newHashSet();
    for (Map.Entry<Integer, Long> entry : nextOffsets.entrySet()) {
      final long endOffset = endOffsets.get(entry.getKey());
      if (entry.getValue() < endOffset) {
        assignment.add(entry.getKey());
      } else if (entry.getValue() == endOffset) {
        log.info("Finished reading partition[%d].", entry.getKey());
      } else {
        throw new ISE(
            "WTF?! Cannot start from offset[%,d] > endOffset[%,d]",
            entry.getValue(),
            endOffset
        );
      }
    }

    KafkaIndexTask.assignPartitions(consumer, topic, assignment);

    // Seek to starting offsets.
    for (final int partition : assignment) {
      final long offset = nextOffsets.get(partition);
      log.info("Seeking partition[%d] to offset[%,d].", partition, offset);
      consumer.seek(new TopicPartition(topic, partition), offset);
    }

    return assignment;
  }

  private void checkAndMaybeThrowException()
  {
    if (throwableAtomicReference.get() != null) {
      Throwables.propagate(throwableAtomicReference.get());
    }
  }

  /**
   * Checks if the pauseRequested flag was set and if so blocks:
   * a) if pauseMillis == PAUSE_FOREVER, until pauseRequested is cleared
   * b) if pauseMillis != PAUSE_FOREVER, until pauseMillis elapses -or- pauseRequested is cleared
   * <p/>
   * If pauseMillis is changed while paused, the new pause timeout will be applied. This allows adjustment of the
   * pause timeout (making a timed pause into an indefinite pause and vice versa is valid) without having to resume
   * and ensures that the loop continues to stay paused without ingesting any new events. You will need to signal
   * shouldResume after adjusting pauseMillis for the new value to take effect.
   * <p/>
   * Sets paused = true and signals paused so callers can be notified when the pause command has been accepted.
   * <p/>
   * Additionally, pauses if all partitions assignments have been read and pauseAfterRead flag is set.
   *
   * @return true if a pause request was handled, false otherwise
   */
  private boolean possiblyPause(Set<Integer> assignment) throws InterruptedException
  {
    pauseLock.lockInterruptibly();
    try {
      if (ioConfig.isPauseAfterRead() && assignment.isEmpty()) {
        pauseMillis = KafkaIndexTask.PAUSE_FOREVER;
        pauseRequested = true;
      }

      if (pauseRequested) {
        status = Status.PAUSED;
        long nanos = 0;
        hasPaused.signalAll();

        while (pauseRequested) {
          if (pauseMillis == KafkaIndexTask.PAUSE_FOREVER) {
            log.info("Pausing ingestion until resumed");
            shouldResume.await();
          } else {
            if (pauseMillis > 0) {
              log.info("Pausing ingestion for [%,d] ms", pauseMillis);
              nanos = TimeUnit.MILLISECONDS.toNanos(pauseMillis);
              pauseMillis = 0;
            }
            if (nanos <= 0L) {
              pauseRequested = false; // timeout elapsed
            }
            nanos = shouldResume.awaitNanos(nanos);
          }
        }

        status = Status.READING;
        shouldResume.signalAll();
        log.info("Ingestion loop resumed");
        return true;
      }
    }
    finally {
      pauseLock.unlock();
    }

    return false;
  }

  private void possiblyResetOffsetsOrWait(
      Map<TopicPartition, Long> outOfRangePartitions,
      KafkaConsumer<byte[], byte[]> consumer,
      TaskToolbox taskToolbox
  ) throws InterruptedException, IOException
  {
    final Map<TopicPartition, Long> resetPartitions = Maps.newHashMap();
    boolean doReset = false;
    if (tuningConfig.isResetOffsetAutomatically()) {
      for (Map.Entry<TopicPartition, Long> outOfRangePartition : outOfRangePartitions.entrySet()) {
        final TopicPartition topicPartition = outOfRangePartition.getKey();
        final long nextOffset = outOfRangePartition.getValue();
        // seek to the beginning to get the least available offset
        consumer.seekToBeginning(Collections.singletonList(topicPartition));
        final long leastAvailableOffset = consumer.position(topicPartition);
        // reset the seek
        consumer.seek(topicPartition, nextOffset);
        // Reset consumer offset if resetOffsetAutomatically is set to true
        // and the current message offset in the kafka partition is more than the
        // next message offset that we are trying to fetch
        if (leastAvailableOffset > nextOffset) {
          doReset = true;
          resetPartitions.put(topicPartition, nextOffset);
        }
      }
    }

    if (doReset) {
      sendResetRequestAndWait(resetPartitions, taskToolbox);
    } else {
      log.warn("Retrying in %dms", task.getPollRetryMs());
      pollRetryLock.lockInterruptibly();
      try {
        long nanos = TimeUnit.MILLISECONDS.toNanos(task.getPollRetryMs());
        while (nanos > 0L && !pauseRequested && !stopRequested.get()) {
          nanos = isAwaitingRetry.awaitNanos(nanos);
        }
      }
      finally {
        pollRetryLock.unlock();
      }
    }
  }

  private void handleParseException(ParseException pe, ConsumerRecord<byte[], byte[]> record)
  {
    if (pe.isFromPartiallyValidRow()) {
      rowIngestionMeters.incrementProcessedWithError();
    } else {
      rowIngestionMeters.incrementUnparseable();
    }

    if (tuningConfig.isLogParseExceptions()) {
      log.error(
          pe,
          "Encountered parse exception on row from partition[%d] offset[%d]",
          record.partition(),
          record.offset()
      );
    }

    if (savedParseExceptions != null) {
      savedParseExceptions.add(pe);
    }

    if (rowIngestionMeters.getUnparseable() + rowIngestionMeters.getProcessedWithError()
        > tuningConfig.getMaxParseExceptions()) {
      log.error("Max parse exceptions exceeded, terminating task...");
      throw new RuntimeException("Max parse exceptions exceeded, terminating task...");
    }
  }

  private boolean isPaused()
  {
    return status == Status.PAUSED;
  }

  private void requestPause(long pauseMillis)
  {
    this.pauseMillis = pauseMillis;
    pauseRequested = true;
  }

  private void sendResetRequestAndWait(Map<TopicPartition, Long> outOfRangePartitions, TaskToolbox taskToolbox)
      throws IOException
  {
    Map<Integer, Long> partitionOffsetMap = Maps.newHashMap();
    for (Map.Entry<TopicPartition, Long> outOfRangePartition : outOfRangePartitions.entrySet()) {
      partitionOffsetMap.put(outOfRangePartition.getKey().partition(), outOfRangePartition.getValue());
    }
    boolean result = taskToolbox.getTaskActionClient()
                                .submit(new ResetDataSourceMetadataAction(
                                    task.getDataSource(),
                                    new KafkaDataSourceMetadata(new KafkaPartitions(
                                        ioConfig.getStartPartitions()
                                                .getTopic(),
                                        partitionOffsetMap
                                    ))
                                ));

    if (result) {
      log.makeAlert("Resetting Kafka offsets for datasource [%s]", task.getDataSource())
         .addData("partitions", partitionOffsetMap.keySet())
         .emit();
      // wait for being killed by supervisor
      requestPause(KafkaIndexTask.PAUSE_FOREVER);
    } else {
      log.makeAlert("Failed to send reset request for partitions [%s]", partitionOffsetMap.keySet()).emit();
    }
  }

  /**
   * Authorizes action to be performed on this task's datasource
   *
   * @return authorization result
   */
  private Access authorizationCheck(final HttpServletRequest req, Action action)
  {
    return IndexTaskUtils.datasourceAuthorizationCheck(req, action, task.getDataSource(), authorizerMapper);
  }

  @Override
  public Appenderator getAppenderator()
  {
    return appenderator;
  }

  @Override
  public RowIngestionMeters getRowIngestionMeters()
  {
    return rowIngestionMeters;
  }

  @Override
  public FireDepartmentMetrics getFireDepartmentMetrics()
  {
    return fireDepartmentMetrics;
  }

  @Override
  public void stopGracefully()
  {
    log.info("Stopping gracefully (status: [%s])", status);
    stopRequested.set(true);

    synchronized (statusLock) {
      if (status == Status.PUBLISHING) {
        runThread.interrupt();
        return;
      }
    }

    try {
      if (pauseLock.tryLock(KafkaIndexTask.LOCK_ACQUIRE_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        try {
          if (pauseRequested) {
            pauseRequested = false;
            shouldResume.signalAll();
          }
        }
        finally {
          pauseLock.unlock();
        }
      } else {
        log.warn("While stopping: failed to acquire pauseLock before timeout, interrupting run thread");
        runThread.interrupt();
        return;
      }

      if (pollRetryLock.tryLock(KafkaIndexTask.LOCK_ACQUIRE_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        try {
          isAwaitingRetry.signalAll();
        }
        finally {
          pollRetryLock.unlock();
        }
      } else {
        log.warn("While stopping: failed to acquire pollRetryLock before timeout, interrupting run thread");
        runThread.interrupt();
      }
    }
    catch (Exception e) {
      Throwables.propagate(e);
    }
  }

  @POST
  @Path("/stop")
  public Response stop(@Context final HttpServletRequest req)
  {
    authorizationCheck(req, Action.WRITE);
    stopGracefully();
    return Response.status(Response.Status.OK).build();
  }

  @GET
  @Path("/status")
  @Produces(MediaType.APPLICATION_JSON)
  public Status getStatusHTTP(@Context final HttpServletRequest req)
  {
    authorizationCheck(req, Action.READ);
    return status;
  }

  @Override
  public Status getStatus()
  {
    return status;
  }

  @GET
  @Path("/offsets/current")
  @Produces(MediaType.APPLICATION_JSON)
  public Map<Integer, Long> getCurrentOffsets(@Context final HttpServletRequest req)
  {
    authorizationCheck(req, Action.READ);
    return getCurrentOffsets();
  }

  @Override
  public Map<Integer, Long> getCurrentOffsets()
  {
    return nextOffsets;
  }

  @GET
  @Path("/offsets/end")
  @Produces(MediaType.APPLICATION_JSON)
  public Map<Integer, Long> getEndOffsetsHTTP(@Context final HttpServletRequest req)
  {
    authorizationCheck(req, Action.READ);
    return getEndOffsets();
  }

  @Override
  public Map<Integer, Long> getEndOffsets()
  {
    return endOffsets;
  }

  @POST
  @Path("/offsets/end")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response setEndOffsetsHTTP(
      Map<Integer, Long> offsets,
      @QueryParam("resume") @DefaultValue("false") final boolean resume,
      @QueryParam("finish") @DefaultValue("true") final boolean finish,
      // this field is only for internal purposes, shouldn't be usually set by users
      @Context final HttpServletRequest req
  ) throws InterruptedException
  {
    authorizationCheck(req, Action.WRITE);
    return setEndOffsets(offsets, resume, finish);
  }

  @GET
  @Path("/rowStats")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getRowStats(
      @Context final HttpServletRequest req
  )
  {
    authorizationCheck(req, Action.READ);
    Map<String, Object> returnMap = Maps.newHashMap();
    Map<String, Object> totalsMap = Maps.newHashMap();
    Map<String, Object> averagesMap = Maps.newHashMap();

    totalsMap.put(
        RowIngestionMeters.BUILD_SEGMENTS,
        rowIngestionMeters.getTotals()
    );
    averagesMap.put(
        RowIngestionMeters.BUILD_SEGMENTS,
        rowIngestionMeters.getMovingAverages()
    );

    returnMap.put("movingAverages", averagesMap);
    returnMap.put("totals", totalsMap);
    return Response.ok(returnMap).build();
  }

  @GET
  @Path("/unparseableEvents")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getUnparseableEvents(
      @Context final HttpServletRequest req
  )
  {
    authorizationCheck(req, Action.READ);
    List<String> events = IndexTaskUtils.getMessagesFromSavedParseExceptions(savedParseExceptions);
    return Response.ok(events).build();
  }

  @Override
  public Response setEndOffsets(
      Map<Integer, Long> offsets,
      final boolean resume,
      final boolean finish // this field is only for internal purposes, shouldn't be usually set by users
  ) throws InterruptedException
  {
    if (offsets == null) {
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity("Request body must contain a map of { partition:endOffset }")
                     .build();
    } else if (!endOffsets.keySet().containsAll(offsets.keySet())) {
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity(
                         StringUtils.format(
                             "Request contains partitions not being handled by this task, my partitions: %s",
                             endOffsets.keySet()
                         )
                     )
                     .build();
    } else {
      try {
        pauseLock.lockInterruptibly();
        // Perform all sequence related checks before checking for isPaused()
        // and after acquiring pauseLock to correctly guard against duplicate requests
        Preconditions.checkState(sequences.size() > 0, "WTH?! No Sequences found to set end offsets");

        final SequenceMetadata latestSequence = sequences.get(sequences.size() - 1);
        if ((latestSequence.getStartOffsets().equals(offsets) && !finish) ||
            (latestSequence.getEndOffsets().equals(offsets) && finish)) {
          log.warn("Ignoring duplicate request, end offsets already set for sequences [%s]", sequences);
          return Response.ok(offsets).build();
        } else if (latestSequence.isCheckpointed() && !ioConfig.isPauseAfterRead()) {
          return Response.status(Response.Status.BAD_REQUEST)
                         .entity(StringUtils.format(
                             "WTH?! Sequence [%s] has already endOffsets set, cannot set to [%s]",
                             latestSequence,
                             offsets
                         )).build();
        } else if (!isPaused()) {
          return Response.status(Response.Status.BAD_REQUEST)
                         .entity("Task must be paused before changing the end offsets")
                         .build();
        }

        for (Map.Entry<Integer, Long> entry : offsets.entrySet()) {
          if (entry.getValue().compareTo(nextOffsets.get(entry.getKey())) < 0) {
            return Response.status(Response.Status.BAD_REQUEST)
                           .entity(
                               StringUtils.format(
                                   "End offset must be >= current offset for partition [%s] (current: %s)",
                                   entry.getKey(),
                                   nextOffsets.get(entry.getKey())
                               )
                           )
                           .build();
          }
        }

        resetNextCheckpointTime();
        latestSequence.setEndOffsets(offsets);

        if (finish) {
          log.info("Updating endOffsets from [%s] to [%s]", endOffsets, offsets);
          endOffsets.putAll(offsets);
        } else {
          Preconditions.checkState(!ioConfig.isPauseAfterRead());
          // create new sequence
          final SequenceMetadata newSequence = new SequenceMetadata(
              latestSequence.getSequenceId() + 1,
              StringUtils.format("%s_%d", ioConfig.getBaseSequenceName(), latestSequence.getSequenceId() + 1),
              offsets,
              endOffsets,
              false
          );
          sequences.add(newSequence);
        }

        persistSequences();
      }
      catch (Exception e) {
        log.error(e, "Unable to set end offsets, dying");
        throwableAtomicReference.set(e);
        Throwables.propagate(e);
      }
      finally {
        pauseLock.unlock();
      }
    }

    if (resume) {
      resume();
    }

    return Response.ok(offsets).build();
  }

  private void resetNextCheckpointTime()
  {
    nextCheckpointTime = DateTimes.nowUtc().plus(tuningConfig.getIntermediateHandoffPeriod()).getMillis();
  }

  @GET
  @Path("/checkpoints")
  @Produces(MediaType.APPLICATION_JSON)
  public Map<Integer, Map<Integer, Long>> getCheckpointsHTTP(@Context final HttpServletRequest req)
  {
    authorizationCheck(req, Action.READ);
    return getCheckpoints();
  }

  private Map<Integer, Map<Integer, Long>> getCheckpoints()
  {
    TreeMap<Integer, Map<Integer, Long>> result = new TreeMap<>();
    result.putAll(
        sequences.stream().collect(Collectors.toMap(SequenceMetadata::getSequenceId, SequenceMetadata::getStartOffsets))
    );
    return result;
  }

  /**
   * Signals the ingestion loop to pause.
   *
   * @param timeout how long to pause for before resuming in milliseconds, <= 0 means indefinitely
   *
   * @return one of the following Responses: 400 Bad Request if the task has started publishing; 202 Accepted if the
   * method has timed out and returned before the task has paused; 200 OK with a map of the current partition offsets
   * in the response body if the task successfully paused
   */
  @POST
  @Path("/pause")
  @Produces(MediaType.APPLICATION_JSON)
  public Response pauseHTTP(
      @QueryParam("timeout") @DefaultValue("0") final long timeout,
      @Context final HttpServletRequest req
  ) throws InterruptedException
  {
    authorizationCheck(req, Action.WRITE);
    return pause(timeout);
  }

  @Override
  public Response pause(final long timeout) throws InterruptedException
  {
    if (!(status == Status.PAUSED || status == Status.READING)) {
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity(StringUtils.format("Can't pause, task is not in a pausable state (state: [%s])", status))
                     .build();
    }

    pauseLock.lockInterruptibly();
    try {
      pauseMillis = timeout <= 0 ? KafkaIndexTask.PAUSE_FOREVER : timeout;
      pauseRequested = true;

      pollRetryLock.lockInterruptibly();
      try {
        isAwaitingRetry.signalAll();
      }
      finally {
        pollRetryLock.unlock();
      }

      if (isPaused()) {
        shouldResume.signalAll(); // kick the monitor so it re-awaits with the new pauseMillis
      }

      long nanos = TimeUnit.SECONDS.toNanos(2);
      while (!isPaused()) {
        if (nanos <= 0L) {
          return Response.status(Response.Status.ACCEPTED)
                         .entity("Request accepted but task has not yet paused")
                         .build();
        }
        nanos = hasPaused.awaitNanos(nanos);
      }
    }
    finally {
      pauseLock.unlock();
    }

    try {
      return Response.ok().entity(toolbox.getObjectMapper().writeValueAsString(getCurrentOffsets())).build();
    }
    catch (JsonProcessingException e) {
      throw Throwables.propagate(e);
    }
  }

  @POST
  @Path("/resume")
  public Response resumeHTTP(@Context final HttpServletRequest req) throws InterruptedException
  {
    authorizationCheck(req, Action.WRITE);
    resume();
    return Response.status(Response.Status.OK).build();
  }

  @Override
  public void resume() throws InterruptedException
  {
    pauseLock.lockInterruptibly();
    try {
      pauseRequested = false;
      shouldResume.signalAll();

      long nanos = TimeUnit.SECONDS.toNanos(5);
      while (isPaused()) {
        if (nanos <= 0L) {
          throw new RuntimeException("Resume command was not accepted within 5 seconds");
        }
        nanos = shouldResume.awaitNanos(nanos);
      }
    }
    finally {
      pauseLock.unlock();
    }
  }

  @GET
  @Path("/time/start")
  @Produces(MediaType.APPLICATION_JSON)
  public DateTime getStartTime(@Context final HttpServletRequest req)
  {
    authorizationCheck(req, Action.WRITE);
    return startTime;
  }

  private static class SequenceMetadata
  {
    private final int sequenceId;
    private final String sequenceName;
    private final Map<Integer, Long> startOffsets;
    private final Map<Integer, Long> endOffsets;
    private final Set<Integer> assignments;
    private final boolean sentinel;
    private volatile boolean checkpointed;

    @JsonCreator
    public SequenceMetadata(
        @JsonProperty("sequenceId") int sequenceId,
        @JsonProperty("sequenceName") String sequenceName,
        @JsonProperty("startOffsets") Map<Integer, Long> startOffsets,
        @JsonProperty("endOffsets") Map<Integer, Long> endOffsets,
        @JsonProperty("checkpointed") boolean checkpointed
    )
    {
      Preconditions.checkNotNull(sequenceName);
      Preconditions.checkNotNull(startOffsets);
      Preconditions.checkNotNull(endOffsets);
      this.sequenceId = sequenceId;
      this.sequenceName = sequenceName;
      this.startOffsets = ImmutableMap.copyOf(startOffsets);
      this.endOffsets = Maps.newHashMap(endOffsets);
      this.assignments = Sets.newHashSet(startOffsets.keySet());
      this.checkpointed = checkpointed;
      this.sentinel = false;
    }

    @JsonProperty
    public int getSequenceId()
    {
      return sequenceId;
    }

    @JsonProperty
    public boolean isCheckpointed()
    {
      return checkpointed;
    }

    @JsonProperty
    public String getSequenceName()
    {
      return sequenceName;
    }

    @JsonProperty
    public Map<Integer, Long> getStartOffsets()
    {
      return startOffsets;
    }

    @JsonProperty
    public Map<Integer, Long> getEndOffsets()
    {
      return endOffsets;
    }

    @JsonProperty
    public boolean isSentinel()
    {
      return sentinel;
    }

    void setEndOffsets(Map<Integer, Long> newEndOffsets)
    {
      endOffsets.putAll(newEndOffsets);
      checkpointed = true;
    }

    void updateAssignments(Map<Integer, Long> nextPartitionOffset)
    {
      assignments.clear();
      nextPartitionOffset.entrySet().forEach(partitionOffset -> {
        if (Longs.compare(endOffsets.get(partitionOffset.getKey()), nextPartitionOffset.get(partitionOffset.getKey()))
            > 0) {
          assignments.add(partitionOffset.getKey());
        }
      });
    }

    boolean isOpen()
    {
      return !assignments.isEmpty();
    }

    boolean canHandle(ConsumerRecord<byte[], byte[]> record)
    {
      return isOpen()
             && endOffsets.get(record.partition()) != null
             && record.offset() >= startOffsets.get(record.partition())
             && record.offset() < endOffsets.get(record.partition());
    }

    private SequenceMetadata()
    {
      this.sequenceId = -1;
      this.sequenceName = null;
      this.startOffsets = null;
      this.endOffsets = null;
      this.assignments = null;
      this.checkpointed = true;
      this.sentinel = true;
    }

    static SequenceMetadata getSentinelSequenceMetadata()
    {
      return new SequenceMetadata();
    }

    @Override
    public String toString()
    {
      return "SequenceMetadata{" +
             "sequenceName='" + sequenceName + '\'' +
             ", sequenceId=" + sequenceId +
             ", startOffsets=" + startOffsets +
             ", endOffsets=" + endOffsets +
             ", assignments=" + assignments +
             ", sentinel=" + sentinel +
             ", checkpointed=" + checkpointed +
             '}';
    }

    Supplier<Committer> getCommitterSupplier(String topic, Map<Integer, Long> lastPersistedOffsets)
    {
      // Set up committer.
      return () ->
          new Committer()
          {
            @Override
            public Object getMetadata()
            {
              Preconditions.checkState(
                  assignments.isEmpty(),
                  "This committer can be used only once all the records till offsets [%s] have been consumed, also make sure to call updateAssignments before using this committer",
                  endOffsets
              );

              // merge endOffsets for this sequence with globally lastPersistedOffsets
              // This is done because this committer would be persisting only sub set of segments
              // corresponding to the current sequence. Generally, lastPersistedOffsets should already
              // cover endOffsets but just to be sure take max of offsets and persist that
              for (Map.Entry<Integer, Long> partitionOffset : endOffsets.entrySet()) {
                lastPersistedOffsets.put(partitionOffset.getKey(), Math.max(
                    partitionOffset.getValue(),
                    lastPersistedOffsets.getOrDefault(partitionOffset.getKey(), 0L)
                ));
              }

              // Publish metadata can be different from persist metadata as we are going to publish only
              // subset of segments
              return ImmutableMap.of(METADATA_NEXT_PARTITIONS, new KafkaPartitions(topic, lastPersistedOffsets),
                                     METADATA_PUBLISH_PARTITIONS, new KafkaPartitions(topic, endOffsets)
              );
            }

            @Override
            public void run()
            {
              // Do nothing.
            }
          };
    }

    TransactionalSegmentPublisher getPublisher(TaskToolbox toolbox, boolean useTransaction)
    {
      return (segments, commitMetadata) -> {
        final KafkaPartitions finalPartitions = toolbox.getObjectMapper().convertValue(
            ((Map) Preconditions.checkNotNull(commitMetadata, "commitMetadata")).get(METADATA_PUBLISH_PARTITIONS),
            KafkaPartitions.class
        );

        // Sanity check, we should only be publishing things that match our desired end state.
        if (!getEndOffsets().equals(finalPartitions.getPartitionOffsetMap())) {
          throw new ISE(
              "WTF?! Driver for sequence [%s], attempted to publish invalid metadata[%s].",
              toString(),
              commitMetadata
          );
        }

        final SegmentTransactionalInsertAction action;

        if (useTransaction) {
          action = new SegmentTransactionalInsertAction(
              segments,
              new KafkaDataSourceMetadata(new KafkaPartitions(finalPartitions.getTopic(), getStartOffsets())),
              new KafkaDataSourceMetadata(finalPartitions)
          );
        } else {
          action = new SegmentTransactionalInsertAction(segments, null, null);
        }

        log.info("Publishing with isTransaction[%s].", useTransaction);

        return toolbox.getTaskActionClient().submit(action).isSuccess();
      };
    }
  }
}
