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
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
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
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
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
    private Duration authExceptionRetryInterval;

    private final String threadName;

    private final Consumer<RecordBatchIterator<K, V>> handler;
    private final ExecutorService handlerTaskExecutor;
    private HandlerTask handlerTask;
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
        this(topicNames, consumerConfig, handler, KafkaConsumer::new);
    }

    public AdvancedConsumer(
            Collection<String> topicNames,
            Map<String, Object> consumerConfig,
            Consumer<RecordBatchIterator<K, V>> handler,
            Function<Map<String, Object>, KafkaConsumer<K, V>> consumerFactory) {
        Validate.notNull(handler, "Handler is null");
        Validate.notNull(consumerFactory, "Consumer factory is null");

        subscribe(topicNames);

        this.consumerConfig = ConsumerConfigBuilder.
                with(consumerConfig).
                minRequiredConfigs().
                enableAutoCommitDisabled().
                build();
        this.groupId = (String)this.consumerConfig.get(ConsumerConfig.GROUP_ID_CONFIG);
        this.handler = handler;

        var authExceptionRetryIntervalMs = (Long) this.consumerConfig.get(ConsumerConfigBuilder.AUTH_EXCEPTION_RETRY_INTERVAL_MS);
        if (authExceptionRetryIntervalMs != null) {
            this.authExceptionRetryInterval = Duration.ofMillis(authExceptionRetryIntervalMs);
        }

        threadName = buildThreadName();

        handlerTaskExecutor = initHandlerTaskExecutor();

        consumer = consumerFactory.apply(this.consumerConfig);

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

                            if (handlerTaskContainsAnyPartitions(partitions)) {
                                forceCompleteHandlerTask();
                            }

                            consumer.pause(partitions);
                        }
                        @Override
                        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                            LOGGER.debug("Group [{}]: partitions assigned = {}", groupId, partitions);

                            consumer.pause(partitions);
                        }
                    });

                    if (consumer.subscription().isEmpty()) {
                        continue;
                    }

                    if (handlerTaskExists()) {
                        consumer.pause(consumer.assignment());
                    } else {
                        consumer.resume(consumer.assignment());
                    }

                    ConsumerRecords<K, V> records = consumer.poll(POLL_TIMEOUT);
                    if (!records.isEmpty()) {
                        submitNewHandlerTask(records);
                    }
                } catch (AuthorizationException ae) {
                    if (authExceptionRetryInterval == null) {
                        LOGGER.error(String.format("Group [%s]: consumer failed due to Authorization Exception. " +
                                "No authExceptionRetryInterval, will not retry.", groupId), ae);
                        throw ae;
                    } else {
                        LOGGER.error(String.format("Group [%s]: consumer failed due to Authorization Exception. " +
                                "Will retry in %s ms", groupId, authExceptionRetryInterval.toMillis()), ae);
                        sleepFor(authExceptionRetryInterval);
                    }
                } finally {
                    completeHandlerTaskIfDone();
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
        if (handlerTask != null) {
            throw new IllegalStateException("Handler task already exists");
        }

        handlerTask = new HandlerTask(records);
        handlerTaskFuture = handlerTaskExecutor.submit(handlerTask);
    }

    public boolean handlerTaskContainsAnyPartitions(Collection<TopicPartition> partitions) {
        if (handlerTask == null) {
            return false;
        }

        return handlerTask.containsAnyPartitions(partitions);
    }

    private boolean handlerTaskExists() {
        return handlerTask != null;
    }

    private void forceCompleteHandlerTask() {
        if (handlerTask == null) {
            return;
        }

        handlerTask.interrupt();

        commitOffsetsFromHandlerTask(false);

        handlerTask = null;
        handlerTaskFuture = null;
    }

    private void completeHandlerTaskIfDone() {
        if (handlerTaskFuture == null || !handlerTaskFuture.isDone()) {
            return;
        }

        commitOffsetsFromHandlerTask(true);

        handlerTask = null;
        handlerTaskFuture = null;
    }

    private void commitOffsetsFromHandlerTask(boolean waitForComplete) {
        Map<TopicPartition, Long> offsets = getOffsetsFromHandlerTask(waitForComplete);
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

    private Map<TopicPartition, Long> getOffsetsFromHandlerTask(boolean waitForComplete) {
        if (handlerTask == null) {
            return Collections.emptyMap();
        }

        if (waitForComplete) {
            try {
                return handlerTaskFuture.get();
            } catch (InterruptedException ie) {
                throw new RuntimeException("Handler task interrupted", ie);
            } catch (ExecutionException ee) {
                throw new RuntimeException("Handler task failed", ee.getCause());
            }
        } else {
            return handlerTask.getCurrentOffsetsToCommit();
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

    private void sleepFor(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException ie) {
            LOGGER.warn("Group [{}]: interrupted while sleeping", groupId);
            Thread.currentThread().interrupt();
        }
    }

    private static String buildThreadName() {
        return "AdvConsumer-" + THREAD_IDX.incrementAndGet();
    }

    private class HandlerTask implements Callable<Map<TopicPartition, Long>> {

        private final DefaultRecordBatchIterator<K, V> iterator;

        public HandlerTask(ConsumerRecords<K, V> records) {
            iterator = new DefaultRecordBatchIterator<>(records);
        }

        public void interrupt() {
            iterator.interrupt();
        }

        public Map<TopicPartition, Long> getCurrentOffsetsToCommit() {
            return iterator.buildOffsetsToCommit();
        }

        public boolean containsAnyPartitions(Collection<TopicPartition> partitions) {
            if (partitions.isEmpty()) {
                return false;
            }

            return CollectionUtils.containsAny(iterator.getRecords().partitions(), partitions);
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
