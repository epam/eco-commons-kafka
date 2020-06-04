/*
 * Copyright 2020 EPAM Systems
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.  You may obtain a copy
 * of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.epam.eco.commons.kafka.consumer.bootstrap;

import java.io.Closeable;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.epam.eco.commons.kafka.KafkaUtils;
import com.epam.eco.commons.kafka.OffsetRange;
import com.epam.eco.commons.kafka.config.ConsumerConfigBuilder;
import com.epam.eco.commons.kafka.helpers.TopicOffsetRangeFetcher;

/**
 * @author Andrei_Tytsik
 */
public final class BootstrapConsumer<K, V, R> implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(BootstrapConsumer.class);

    private static final OffsetInitializer DEFAULT_OFFSET_INITIALIZER = BeginningOffsetInitializer.INSTANCE;
    private static final long DEFAULT_BOOTSTRAP_TIMEOUT_MS = 1 * 60 * 1000;

    private static final Duration BOOTSTRAP_POLL_TIMEOUT = Duration.of(100, ChronoUnit.MILLIS);
    private static final Duration FETCH_POLL_TIMEOUT = Duration.of(Long.MAX_VALUE, ChronoUnit.MILLIS);

    private final String topicName;
    private final Map<String, Object> consumerConfig;
    private final long bootstrapTimeoutInMs;
    private final RecordCollector<K, V, R> recordCollector;
    private final int instanceCount;
    private final int instanceIndex;

    private final KafkaConsumer<K, V> consumer;
    private final OffsetInitializer offsetInitializer;

    private Set<TopicPartition> partitions;

    private boolean bootstrapDone = false;

    private BootstrapConsumer(
            String topicName,
            Map<String, Object> consumerConfig,
            OffsetInitializer offsetInitializer,
            long bootstrapTimeoutInMs,
            RecordCollector<K, V, R> recordCollector,
            int instanceCount,
            int instanceIndex) {
        Validate.notBlank(topicName, "Topic name is blank");
        Validate.notNull(offsetInitializer, "Offset Initializer is null");
        Validate.isTrue(bootstrapTimeoutInMs > 0, "Bootstrap timeout is invalid");
        Validate.notNull(recordCollector, "Record Collector is null");
        Validate.isTrue(instanceCount > 0, "Instance count is invalid");
        Validate.isTrue(
                instanceIndex >= 0 && instanceIndex < instanceCount,
                "Instance index is invalid");

        this.topicName = topicName;
        this.consumerConfig = ConsumerConfigBuilder.
                with(consumerConfig).
                minRequiredConfigs().
                enableAutoCommitDisabled().
                autoOffsetResetEarliest().
                build();
        this.offsetInitializer = offsetInitializer;
        this.bootstrapTimeoutInMs = bootstrapTimeoutInMs;
        this.recordCollector = recordCollector;
        this.instanceCount = instanceCount;
        this.instanceIndex = instanceIndex;

        consumer = new KafkaConsumer<>(this.consumerConfig);

        LOGGER.info("Initialized");
    }

    public R fetch() {
        assignAndInitPartitionsIfNeeded();

        if (isBootstrapDone()) {
            return fetchUpdates();
        } else {
            return fetchBootstrap();
        }
    }

    public void wakeup() {
        consumer.wakeup();
    }

    public String getTopicName() {
        return topicName;
    }

    public long getBootstrapTimeoutInMs() {
        return bootstrapTimeoutInMs;
    }

    private boolean isBootstrapDone() {
        return bootstrapDone;
    }

    private void setBootstrapDone() {
        bootstrapDone = true;
    }

    private R fetchBootstrap() {
        long bootstrapStartTs = System.currentTimeMillis();
        long bootstrapRecordCount = 0;

        LOGGER.info("Topic [{}]: starting bootstrap", topicName);

        try {
            Map<TopicPartition, Long> latestOffsets = fetchLatestReadableOffsets();
            if (latestOffsets.isEmpty()) {
                LOGGER.info("Topic [{}]: finishing bootstrap, no records to fetch", topicName);
            } else {
                Map<TopicPartition, Long> consumedOffsets = new HashMap<>();
                long statusLogInterval = bootstrapTimeoutInMs / 100;
                long lastStatusLogTs = bootstrapStartTs;
                while (true) {
                    ConsumerRecords<K,V> records = consumer.poll(BOOTSTRAP_POLL_TIMEOUT);

                    long batchRecordCount = records.count();
                    if (batchRecordCount > 0) {
                        bootstrapRecordCount += batchRecordCount;

                        if (System.currentTimeMillis() - lastStatusLogTs > statusLogInterval) {
                            LOGGER.info(
                                    "Topic [{}]: {} bootstrap records fetched",
                                    topicName,
                                    bootstrapRecordCount);
                            lastStatusLogTs = System.currentTimeMillis();
                        }

                        recordCollector.collect(records);

                        consumedOffsets.putAll(KafkaUtils.getConsumerPositions(consumer));

                        if (compareOffsetsGreaterOrEqual(consumedOffsets, latestOffsets)) {
                            LOGGER.info(
                                    "Topic [{}]: finishing bootstrap, received offsets have met expected threshold",
                                    topicName);
                            break;
                        }
                    }

                    if (System.currentTimeMillis() - bootstrapStartTs > bootstrapTimeoutInMs) {
                        LOGGER.info(
                                "Topic [{}]: finishing bootstrap, timeout has exceeded", topicName);
                        break;
                    }
                }
            }
        } catch (WakeupException wue) {
            LOGGER.warn("Topic [{}]: bootstrap aborted (woken up)", topicName);
        } catch (Exception ex) {
            LOGGER.error(String.format("Topic [%s]: bootstrap failed", topicName), ex);
            throw ex;
        } finally {
            setBootstrapDone();
        }

        LOGGER.info(
                "Topic [{}]: bootstrap done in {}, {} records fetched",
                topicName,
                DurationFormatUtils.formatDurationHMS(System.currentTimeMillis() - bootstrapStartTs),
                bootstrapRecordCount);

        return recordCollector.result();
    }

    private R fetchUpdates() {
        try {
            ConsumerRecords<K,V> records = consumer.poll(FETCH_POLL_TIMEOUT);

            LOGGER.debug("Topic [{}]: {} update records fetched", topicName, records.count());

            recordCollector.collect(records);
        } catch (WakeupException wue) {
            LOGGER.warn("Topic [{}]: update aborted (woken up)", topicName);
        } catch (Exception ex) {
            LOGGER.error(String.format("Topic [%s]: update failed", topicName), ex);
            throw ex;
        }

        return recordCollector.result();
    }

    private void assignAndInitPartitionsIfNeeded() {
        if (partitions != null) { // already done
            return;
        }

        List<TopicPartition> partitionsAll = KafkaUtils.getTopicPartitionsAsList(consumer, topicName);
        if (instanceCount > partitionsAll.size()) {
            throw new RuntimeException(
                    String.format(
                            "Instance count %d is larger than actual number of topic [%s] partitions %d",
                            instanceCount, topicName, partitionsAll.size()));
        }

        partitions = partitionsAll.
                stream().
                filter(partition -> partition.partition() % instanceCount == instanceIndex).
                collect(Collectors.toSet());

        consumer.assign(partitions);
        offsetInitializer.init(consumer, partitions);
    }

    private Map<TopicPartition, Long> fetchLatestReadableOffsets() {
        Map<TopicPartition, OffsetRange> offsets = TopicOffsetRangeFetcher.
                with(consumerConfig).
                fetchForPartitions(partitions);
        return offsets.entrySet().stream().
                filter(e -> e.getValue().getSize() > 0).
                filter(e -> e.getValue().contains(consumer.position(e.getKey()))).
                collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().getLargest()));
    }

    private boolean compareOffsetsGreaterOrEqual(
            Map<TopicPartition, Long> topicOffsets1,
            Map<TopicPartition, Long> topicOffsets2) {
        for (Map.Entry<TopicPartition, Long> entry : topicOffsets2.entrySet()) {
            Long offset1 = topicOffsets1.get(entry.getKey());
            Long offset2 = entry.getValue();
            if (offset1 == null || offset1 < offset2) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void close() {
        KafkaUtils.closeQuietly(consumer);
    }

    public static <K, V, R> Builder<K, V, R> builder() {
        return new Builder<>();
    }

    public static class Builder<K, V, R> {

        private String topicName;
        private Map<String, Object> consumerConfig;
        private ConsumerConfigBuilder consumerConfigBuilder;
        private OffsetInitializer offsetInitializer = DEFAULT_OFFSET_INITIALIZER;
        private long bootstrapTimeoutInMs = DEFAULT_BOOTSTRAP_TIMEOUT_MS;
        private RecordCollector<K, V, R> recordCollector;
        private int instanceCount = 1;
        private int instanceIndex = 0;

        public Builder<K, V, R> topicName(String topicName) {
            this.topicName = topicName;
            return this;
        }
        public Builder<K, V, R> consumerConfig(Map<String, Object> consumerConfig) {
            this.consumerConfig = consumerConfig;
            return this;
        }
        public Builder<K, V, R> consumerConfigBuilder(ConsumerConfigBuilder consumerConfigBuilder) {
            this.consumerConfigBuilder = consumerConfigBuilder;
            return this;
        }
        public Builder<K, V, R> offsetInitializer(OffsetInitializer offsetInitializer) {
            this.offsetInitializer = offsetInitializer;
            return this;
        }
        public Builder<K, V, R> bootstrapTimeoutInMs(long bootstrapTimeoutInMs) {
            this.bootstrapTimeoutInMs = bootstrapTimeoutInMs;
            return this;
        }
        public Builder<K, V, R> recordCollector(RecordCollector<K, V, R> recordCollector) {
            this.recordCollector = recordCollector;
            return this;
        }
        public Builder<K, V, R> instanceCount(int instanceCount) {
            this.instanceCount = instanceCount;
            return this;
        }
        public Builder<K, V, R> instanceIndex(int instanceIndex) {
            this.instanceIndex = instanceIndex;
            return this;
        }

        public BootstrapConsumer<K, V, R> build() {
            return new BootstrapConsumer<>(
                    topicName,
                    consumerConfigBuilder != null ? consumerConfigBuilder.build() : consumerConfig,
                    offsetInitializer,
                    bootstrapTimeoutInMs,
                    recordCollector,
                    instanceCount,
                    instanceIndex);
        }

    }

}
