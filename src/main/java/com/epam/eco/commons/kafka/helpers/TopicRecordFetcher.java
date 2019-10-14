/*
 * Copyright 2019 EPAM Systems
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
package com.epam.eco.commons.kafka.helpers;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import com.epam.eco.commons.kafka.KafkaUtils;
import com.epam.eco.commons.kafka.OffsetRange;
import com.epam.eco.commons.kafka.TopicPartitionComparator;
import com.epam.eco.commons.kafka.config.ConsumerConfigBuilder;

/**
 * @author Andrei_Tytsik
 */
public class TopicRecordFetcher<K, V> {

    private static final Duration POLL_TIMEOUT = Duration.of(100, ChronoUnit.MILLIS);

    private final Map<String, Object> consumerConfig;

    private TopicRecordFetcher(String bootstrapServers, Map<String, Object> consumerConfig) {
        ConsumerConfigBuilder configBuilder = ConsumerConfigBuilder.
                with(consumerConfig).
                minRequiredConfigs().
                enableAutoCommitDisabled().
                autoOffsetResetEarliest().
                clientIdRandom();
        if (bootstrapServers != null) {
            configBuilder.bootstrapServers(bootstrapServers);
        }
        this.consumerConfig = configBuilder.build();
    }

    public static <K, V> TopicRecordFetcher<K, V> with(Map<String, Object> consumerConfig) {
        return new TopicRecordFetcher<>(null, consumerConfig);
    }

    public static <K, V> TopicRecordFetcher<K, V> with(String bootstrapServers) {
        return new TopicRecordFetcher<>(bootstrapServers, null);
    }

    public RecordFetchResult<K, V> fetch(
            String[] topicNames,
            long offset,
            long limit,
            long timeoutInMs) {
        return fetch(topicNames, offset, limit, null, timeoutInMs);
    }

    public RecordFetchResult<K, V> fetch(
            Collection<String> topicNames,
            long offset,
            long limit,
            long timeoutInMs) {
        return fetch(
                topicNames,
                offset,
                limit,
                null,
                timeoutInMs);
    }

    public RecordFetchResult<K, V> fetch(
            Collection<String> topicNames,
            long offset,
            long limit,
            Predicate<ConsumerRecord<K, V>> filter,
            long timeoutInMs) {
        return fetch(
                topicNames != null ? topicNames.stream().toArray(String[]::new) : null,
                offset,
                limit,
                filter,
                timeoutInMs);
    }

    public RecordFetchResult<K, V> fetch(
            String[] topicNames,
            long offset,
            long limit,
            Predicate<ConsumerRecord<K, V>> filter,
            long timeoutInMs) {
        Validate.notEmpty(topicNames, "Array of topic names is null or empty");
        Validate.noNullElements(topicNames, "Array of topic names contains null elements");
        Validate.isTrue(offset >= 0, "Offset is invalid");
        Validate.isTrue(limit > 0, "Limit is invalid");
        Validate.isTrue(timeoutInMs > 0, "Timeout is invalid");

        try (KafkaConsumer<K, V> consumer = new KafkaConsumer<>(consumerConfig)) {
            Map<TopicPartition, Long> offsets = toOffsets(
                    KafkaUtils.getTopicPartitionsAsList(consumer, topicNames),
                    offset);
            return doFetch(consumer, offsets, limit, filter, timeoutInMs);
        }
    }

    public RecordFetchResult<K, V> fetch(
            Map<TopicPartition, Long> offsets,
            long limit,
            long timeoutInMs) {
        return fetch(offsets, limit, null, timeoutInMs);
    }

    public RecordFetchResult<K, V> fetch(
            Map<TopicPartition, Long> offsets,
            long limit,
            Predicate<ConsumerRecord<K, V>> filter,
            long timeoutInMs) {
        validateOffsets(offsets);
        Validate.isTrue(limit > 0, "Limit is invalid");
        Validate.isTrue(timeoutInMs > 0, "Timeout is invalid");

        try (KafkaConsumer<K, V> consumer = new KafkaConsumer<>(consumerConfig)) {
            return doFetch(consumer, offsets, limit, filter, timeoutInMs);
        }
    }

    private RecordFetchResult<K, V> doFetch(
            KafkaConsumer<K, V> consumer,
            Map<TopicPartition, Long> offsets,
            long limit,
            Predicate<ConsumerRecord<K, V>> filter,
            long timeoutMs) {
        Map<TopicPartition, OffsetRange> offsetRanges = fetchOffsetRanges(offsets.keySet());

        offsets = filterOutUselessOffsets(offsets, offsetRanges);
        if (offsets.isEmpty()) {
            return RecordFetchResult.emptyResult();
        }

        Map<TopicPartition, RecordCollector> collectors = initRecordCollectorsForPartitions(
                offsets.keySet(),
                filter,
                limit);

        assignConsumerToPartitionsAndSeekOffsets(consumer, offsets);

        long fetchStart = System.currentTimeMillis();

        Map<TopicPartition, Long> consumedOffsets = new HashMap<>();
        while (true) {
            ConsumerRecords<K, V> records = consumer.poll(POLL_TIMEOUT);
            if (records.count() > 0) {
                collectRecords(records, collectors);

                consumedOffsets.putAll(KafkaUtils.getConsumerPositions(consumer));

                if (
                        areAllCollectorsDone(collectors) ||
                        areAllOffsetsReachedEndOfRange(consumedOffsets, offsetRanges)) {
                    break;
                }
            }

            if (System.currentTimeMillis() - fetchStart > timeoutMs) {
                break;
            }
        }

        return toFetchResult(collectors, offsetRanges);
    }

    private void validateOffsets(Map<TopicPartition, Long> offsets) {
        Validate.notNull(offsets, "Collection of partition offsets is null or empty");
        Validate.noNullElements(offsets.keySet(),
                "Collection of partition offset keys contains null elements");
        Validate.noNullElements(offsets.values(),
                "Collection of partition offset values contains null elements");

        offsets.forEach((key, value) -> {
            if (value < 0) {
                throw new IllegalArgumentException(
                        String.format("Offset for %s is invalid: %s", key, value));
            }
        });
    }

    private boolean collectRecords(
            ConsumerRecords<K, V> records,
            Map<TopicPartition, RecordCollector> collectors) {
        boolean anythingCollected = false;
        for (Entry<TopicPartition, RecordCollector> entry : collectors.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            RecordCollector collector = entry.getValue();
            if (collector.isLimitReached()) {
                continue;
            }
            for (ConsumerRecord<K, V> record : records.records(topicPartition)) {
                if (collector.isLimitReached()) {
                    break;
                }
                collector.add(record);
                anythingCollected = true;
            }
        }
        return anythingCollected;
    }

    private List<TopicPartition> assignConsumerToPartitionsAndSeekOffsets(
            KafkaConsumer<K, V> consumer,
            Map<TopicPartition, Long> offsets) {
        List<TopicPartition> partitions = new ArrayList<>(offsets.keySet());
        consumer.assign(partitions);
        offsets.forEach(consumer::seek);
        return partitions;
    }

    private Map<TopicPartition, RecordCollector> initRecordCollectorsForPartitions(
            Collection<TopicPartition> partitions,
            Predicate<ConsumerRecord<K, V>> filter,
            long limit) {
        if (partitions.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<TopicPartition, RecordCollector> collectors = new TreeMap<>(TopicPartitionComparator.INSTANCE);

        long limitEven = limit / partitions.size();
        long limitOdd = limit % partitions.size();
        for (TopicPartition partition : partitions) {
            long oddPerPartition = 0;
            if (limitOdd > 0) {
                oddPerPartition = 1;
                limitOdd--;
            }
            long limitPerPartition = limitEven + oddPerPartition;
            if (limitPerPartition <= 0) {
                break;
            }
            collectors.put(
                    partition,
                    new RecordCollector(filter, limitPerPartition));
        }
        return collectors;
    }

    private Map<TopicPartition, Long> toOffsets(List<TopicPartition> partitions, long offset) {
        return partitions.stream().
                collect(Collectors.toMap(
                        Function.identity(),
                        partition -> offset));
    }

    private RecordFetchResult<K, V> toFetchResult(
            Map<TopicPartition, RecordCollector> recordCollectors,
            Map<TopicPartition, OffsetRange> offsetRanges) {
        RecordFetchResult.Builder<K, V> builder = RecordFetchResult.builder();
        for (Entry<TopicPartition, RecordCollector> entry : recordCollectors.entrySet()) {
            TopicPartition partition = entry.getKey();
            RecordCollector recordCollector = entry.getValue();

            if (recordCollector.isEmpty()) {
                continue;
            }

            builder.result(
                    PartitionRecordFetchResult.<K, V>builder().
                        partition(partition).
                        addRecords(recordCollector.getRecords()).
                        partitionOffsets(offsetRanges.get(partition)).
                        scannedOffsets(
                                new OffsetRange(
                                        recordCollector.getSmallestScannedOffset(),
                                        recordCollector.getLargestScannedOffset(),
                                        true)).
                        build());
        }
        return builder.build();
    }

    private Map<TopicPartition, Long> filterOutUselessOffsets(
            Map<TopicPartition, Long> offsets,
            Map<TopicPartition, OffsetRange> offsetRanges) {
        return offsets.entrySet().stream().
                filter(entry -> {
                    OffsetRange range = offsetRanges.get(entry.getKey());
                    Long offset = entry.getValue();
                    return
                            range != null && range.getSize() > 0 &&
                            (offset < range.getSmallest() || range.contains(offset));
                }).
                collect(Collectors.toMap(
                        Entry::getKey,
                        Entry::getValue));
    }

    private Map<TopicPartition, OffsetRange> fetchOffsetRanges(Collection<TopicPartition> partitions) {
        return TopicOffsetFetcher.with(consumerConfig).fetchForPartitions(partitions);
    }

    private boolean areAllCollectorsDone(Map<TopicPartition, RecordCollector> collectors) {
        for (RecordCollector collector : collectors.values()) {
            if (!collector.isLimitReached()) {
                return false;
            }
        }
        return true;
    }

    private boolean areAllOffsetsReachedEndOfRange(
            Map<TopicPartition, Long> offsets,
            Map<TopicPartition, OffsetRange> offsetRanges) {
        for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet()) {
            Long offset = entry.getValue();
            OffsetRange range = offsetRanges.get(entry.getKey());
            if (offset < range.getLargest()) {
                return false;
            }
        }
        return true;
    }

    private class RecordCollector {

        private final Predicate<ConsumerRecord<K, V>> filter;
        private final long limit;

        private final List<ConsumerRecord<K, V>> records;

        private long smallestScannedOffset = -1;
        private long largestScannedOffset = -1;

        public RecordCollector(Predicate<ConsumerRecord<K, V>> filter, long limit) {
            this.filter = filter;
            this.limit = limit;
            records = new ArrayList<>((int)limit);
        }

        public void add(ConsumerRecord<K, V> record) {
            if (isLimitReached()) {
                return;
            }

            updateScannedOffsets(record);

            if (passesFilter(record)) {
                records.add(record);
            }
        }

        public boolean isLimitReached() {
            return records.size() >= limit;
        }

        public List<ConsumerRecord<K, V>> getRecords() {
            return records;
        }
        public long getSmallestScannedOffset() {
            return smallestScannedOffset;
        }
        public long getLargestScannedOffset() {
            return largestScannedOffset;
        }
        public boolean isEmpty() {
            return records.isEmpty();
        }

        private boolean passesFilter(ConsumerRecord<K, V> record) {
            return filter == null || filter.test(record);
        }

        private void updateScannedOffsets(ConsumerRecord<K, V> record) {
            if (smallestScannedOffset == -1) {
                smallestScannedOffset = record.offset();
            }
            largestScannedOffset = record.offset();
        }

    }

}
