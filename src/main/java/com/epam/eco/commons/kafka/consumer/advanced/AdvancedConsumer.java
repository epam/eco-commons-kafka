/*******************************************************************************
 *  Copyright 2022 EPAM Systems
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License.  You may obtain a copy
 *  of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 *******************************************************************************/
package com.epam.eco.commons.kafka.consumer.advanced;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.epam.eco.commons.kafka.KafkaUtils;
import com.epam.eco.commons.kafka.config.ConsumerConfigBuilder;

/**
 * @author Andrei_Tytsik
 */
public final class AdvancedConsumer<K, V> extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger(AdvancedConsumer.class);

    private static final AtomicInteger THREAD_IDX = new AtomicInteger(-1);

    private static final long SHUTDOWN_TIMEOUT_SECONDS = 30;

    private static final Duration POLL_TIMEOUT = Duration.of(100, ChronoUnit.MILLIS);

    private final Map<String, Object> consumerConfig;
    private final String groupId;

    private final String threadName;

    private final Consumer<RecordBatchIterator<K, V>> handler;
    private final ExecutorService handlerTaskExecutor;
    private RecordBatchHandlerTask handlerTask;
    private Future<Map<TopicPartition, Long>> handlerTaskFuture;

    private final KafkaConsumer<K, V> consumer;

    private final AtomicBoolean subscriptionChanged = new AtomicBoolean(false);
    private final Set<String> subscription = new HashSet<>();

    private final AtomicBoolean running = new AtomicBoolean(true);
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    public AdvancedConsumer(
            String[] topicNames,
            Map<String, Object> consumerConfig,
            Consumer<RecordBatchIterator<K, V>> handler) {
        this(
                topicNames != null ? Arrays.asList(topicNames) : null,
                consumerConfig,
                handler);
    }

    public AdvancedConsumer(
            Collection<String> topicNames,
            Map<String, Object> consumerConfig,
            Consumer<RecordBatchIterator<K, V>> handler) {
        Validate.notNull(handler, "Handler is null");

        subscribe(topicNames);

        this.consumerConfig = ConsumerConfigBuilder.
                with(consumerConfig).
                minRequiredConfigs().
                enableAutoCommitDisabled().
                build();
        this.groupId = (String)this.consumerConfig.get(ConsumerConfig.GROUP_ID_CONFIG);
        this.handler = handler;

        threadName = buildThreadName();

        handlerTaskExecutor = initHandlerTaskExecutor();

        consumer = new KafkaConsumer<>(this.consumerConfig);

        LOGGER.info("Initialized");
    }

    @Override
    public void run() {
        Thread.currentThread().setName(threadName);

        try {
            while (running.get()) {
                try {
                    changeSubscriptionIfNeeded(() -> new ConsumerRebalanceListener() {
                        @Override
                        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                            LOGGER.debug("Group [{}]: partitions revoked = {}", groupId, partitions);

                            completeHandlerTask();
                        }
                        @Override
                        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                            LOGGER.debug("Group [{}]: partitions assigned = {}", groupId, partitions);
                        }
                    });

                    if (consumer.subscription().isEmpty()) {
                        continue;
                    }

                    ConsumerRecords<K, V> records = consumer.poll(POLL_TIMEOUT);
                    if (records.isEmpty()) {
                        continue;
                    }

                    submitNewHandlerTask(records);
                    sendHeartbeatsUntilHandlerTaskDone();
                } finally {
                    completeHandlerTask();
                }
            }
        } catch (WakeupException wue) {
            LOGGER.warn("Group [{}]: consumer aborted (woken up)", groupId);
        } catch (Exception ex) {
            LOGGER.error(String.format("Group [%s]: consumer failed", groupId), ex);
            throw ex;
        } finally {
            destroyHandlerTaskExecutor();
            shutdownLatch.countDown();
        }
    }

    public void subscribe(String ... topicNames) {
        subscribe(topicNames != null ? Arrays.asList(topicNames) : null);
    }

    public void subscribe(Collection<String> topicNames) {
        if (topicNames == null || topicNames.isEmpty()) {
            return;
        }

        Validate.noNullElements(topicNames);

        synchronized (subscription) {
            subscription.clear();
            subscription.addAll(topicNames);
            subscriptionChanged.set(true);
        }
    }

    public int partitionCount() {
        return partitions().size();
    }

    public List<PartitionInfo> partitions() {
        synchronized (subscription) {
            return subscription.stream().
                    flatMap(topicName -> consumer.partitionsFor(topicName).stream()).
                    collect(Collectors.toList());
        }
    }

    public void shutdown() throws InterruptedException {
        shutdown(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    public void shutdown(long timeout, TimeUnit timeUnit) throws InterruptedException {
        if (running.compareAndSet(true, false)) {
            consumer.wakeup();
            shutdownLatch.await(timeout, timeUnit);
            KafkaUtils.closeQuietly(consumer);
        }

        LOGGER.info("Shutdown");
    }

    private void submitNewHandlerTask(ConsumerRecords<K, V> records) {
        handlerTask = new RecordBatchHandlerTask(records);
        handlerTaskFuture = handlerTaskExecutor.submit(handlerTask);
    }

    private void sendHeartbeatsUntilHandlerTaskDone() {
        consumer.pause(consumer.assignment());
        try {
            while (isHandlerTaskRunning()) {
                consumer.poll(POLL_TIMEOUT);
            }
        } finally {
            consumer.resume(consumer.assignment());
        }
    }

    private boolean isHandlerTaskRunning() {
        return handlerTaskFuture != null && !handlerTaskFuture.isDone();
    }

    private void completeHandlerTask() {
        if (handlerTask == null) {
            return;
        }

        handlerTask.complete();

        commitOffsetsFromHandlerTask();

        handlerTask = null;
        handlerTaskFuture = null;
    }

    private void commitOffsetsFromHandlerTask() {
        Map<TopicPartition, Long> offsets = getOffsetsFromHandlerTask();
        if (offsets.isEmpty()) {
            return;
        }

        Map<TopicPartition, OffsetAndMetadata> offsetsAndMetadata = offsets.entrySet().stream().
                collect(
                        Collectors.toMap(
                                entry -> entry.getKey(),
                                entry -> new OffsetAndMetadata(entry.getValue())));

        LOGGER.debug("Group [{}]: commiting offsets = {}", groupId, offsetsAndMetadata);

        try {
            consumer.commitSync(offsetsAndMetadata);

            LOGGER.debug("Group [{}]: offsets committed successfully", groupId);
        } catch (Exception ex) {
            LOGGER.warn("Group [{}]: failed to commit offsets. Error = {}", groupId, ex.getMessage());
        }
    }

    private Map<TopicPartition, Long> getOffsetsFromHandlerTask() {
        if (handlerTaskFuture == null) {
            return Collections.emptyMap();
        }

        try {
            return handlerTaskFuture.get();
        } catch (InterruptedException ie) {
            throw new RuntimeException("Handler task interrupted", ie);
        } catch (ExecutionException ee) {
            throw new RuntimeException("Handler task failed", ee.getCause());
        }
    }

    private void changeSubscriptionIfNeeded(
            Supplier<ConsumerRebalanceListener> rebalanceListenerSupplier) {
        if (subscriptionChanged.get()) {
            synchronized (subscription) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(
                            "Group [{}]: changing subscription to = {}",
                            groupId, StringUtils.join(subscription, ", "));
                }

                consumer.subscribe(subscription, rebalanceListenerSupplier.get());
                subscriptionChanged.set(false);
            }
        }
    }

    private ExecutorService initHandlerTaskExecutor() {
        ThreadFactory threadFactory = new BasicThreadFactory.Builder().
                namingPattern(threadName + "_h").
                build();
        return Executors.newSingleThreadExecutor(threadFactory);
    }

    private void destroyHandlerTaskExecutor() {
        handlerTaskExecutor.shutdown();
    }

    private static String buildThreadName() {
        return "AdvConsumer-" + THREAD_IDX.incrementAndGet();
    }

    private class RecordBatchHandlerTask implements Callable<Map<TopicPartition, Long>> {

        private final DefaultRecordBatchIterator<K, V> iterator;

        public RecordBatchHandlerTask(ConsumerRecords<K, V> records) {
            iterator = new DefaultRecordBatchIterator<>(records);
        }

        public void complete() {
            iterator.interrupt();
        }

        @Override
        public Map<TopicPartition, Long> call() throws Exception {
            int numberOfRecordsInBatch = 0;
            int numberOfRecordsToCommit = 0;
            Map<TopicPartition, Long> offsetsToCommit = null;
            try {
                numberOfRecordsInBatch = iterator.getRecords().count();

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(
                            "Group [{}]: record batch started. " +
                            "Number of records = {}, smallest offsets = {}, largest offsest = {}.",
                            groupId,
                            numberOfRecordsInBatch,
                            KafkaUtils.extractSmallestOffsets(iterator.getRecords()),
                            KafkaUtils.extractLargestOffsets(iterator.getRecords()));
                }

                handler.accept(iterator);

                numberOfRecordsToCommit = iterator.countRecordsToCommit();
                offsetsToCommit = iterator.buildOffsetsToCommit();

                return offsetsToCommit;
            } finally {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(
                            "Group [{}]: record batch completed. " +
                            "Number of records to commit = {} (of {}), offsets to commit = {}.",
                            groupId,
                            numberOfRecordsToCommit,
                            numberOfRecordsInBatch,
                            offsetsToCommit);
                }
            }
        }

    }

}
