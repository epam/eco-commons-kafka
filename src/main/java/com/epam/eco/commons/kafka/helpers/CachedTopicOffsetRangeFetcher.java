/*******************************************************************************
 *  Copyright 2023 EPAM Systems
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
package com.epam.eco.commons.kafka.helpers;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import com.epam.eco.commons.kafka.KafkaUtils;
import com.epam.eco.commons.kafka.OffsetRange;

import static java.time.OffsetDateTime.now;
import static java.util.Objects.nonNull;

/**
 * @author Mikhail_Vershkov
 */

public class CachedTopicOffsetRangeFetcher extends TopicOffsetRangeFetcher {

    private static final long EXPIRATION_TIME_MIN = 30L;
    private static final Map<TopicPartition, OffsetRangeCacheRecord> offsetRangeCache = new ConcurrentHashMap<>();
    private static final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private static final Lock readLock = lock.readLock();
    private static final Lock writeLock = lock.writeLock();

    private CachedTopicOffsetRangeFetcher(String bootstrapServers, Map<String, Object> consumerConfig) {
        super(bootstrapServers, consumerConfig);
    }

    public static CachedTopicOffsetRangeFetcher with(Map<String, Object> consumerConfig) {
        return new CachedTopicOffsetRangeFetcher(null, consumerConfig);
    }

    public static CachedTopicOffsetRangeFetcher with(String bootstrapServers) {
        return new CachedTopicOffsetRangeFetcher(bootstrapServers, null);
    }

    @Override
    public Map<TopicPartition, OffsetRange> fetchForPartitions(Collection<TopicPartition> partitions) {
        Validate.notEmpty(partitions, "Collection of partitions is null or empty");
        Validate.noNullElements(partitions, "Collection of partitions contains null elements");

        try (KafkaConsumer<?, ?> consumer = new KafkaConsumer<>(consumerConfig)) {
            return calculateIfAbsent(consumer, partitions);
        }
    }

    @Override
    public Map<TopicPartition, OffsetRange> fetchForTopics(String... topicNames) {
        return fetchForTopics(topicNames != null ? Arrays.asList(topicNames) : null);
    }

    @Override
    public Map<TopicPartition, OffsetRange> fetchForTopics(Collection<String> topicNames) {
        Validate.notEmpty(topicNames, "Collection of topic names is null or empty");
        Validate.noNullElements(topicNames, "Collection of topic names contains null elements");

        try (KafkaConsumer<?, ?> consumer = new KafkaConsumer<>(consumerConfig)) {
            List<TopicPartition> partitions =
                    KafkaUtils.getTopicPartitionsAsList(consumer, topicNames);
            return calculateIfAbsent(consumer, partitions);
        }
    }

    Map<TopicPartition, OffsetRange> calculateIfAbsent(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        Map<TopicPartition, OffsetRange> results = new HashMap<>();

        List<TopicPartition> nonCachedPartitions = new ArrayList<>();
        for(TopicPartition partition : partitions) {
            getCacheValue(partition).ifPresentOrElse(
                    offsetRange -> results.put(partition, offsetRange),
                    () -> nonCachedPartitions.add(partition));
        }
        Map<TopicPartition, OffsetRange> calculatedValues = doFetch(consumer, nonCachedPartitions);
        calculatedValues.forEach(CachedTopicOffsetRangeFetcher::putCacheValue);
        results.putAll(calculatedValues);

        return results;
    }

    public static Optional<OffsetRange> getCacheValue(TopicPartition topicPartition) {
        Optional<OffsetRange> offsetRange = Optional.empty();
        readLock.lock();
        try {
            OffsetRangeCacheRecord record = offsetRangeCache.get(topicPartition);
            if(nonNull(record) && OffsetDateTime.now().isBefore(record.getExpirationDate())) {
                offsetRange = Optional.of(record.getOffsetRange());
            }
        } finally {
            readLock.unlock();
        }
        return offsetRange;
    }

    public static void putCacheValue(TopicPartition topicPartition, OffsetRange offsetRange) {
        writeLock.lock();
        try {
            offsetRangeCache.put(topicPartition, new OffsetRangeCacheRecord(offsetRange));
        }
        finally {
            writeLock.unlock();
        }
    }

    public static void removeRecords(Set<TopicPartition> partitionSet) {
        if(!partitionSet.isEmpty()) {
            writeLock.lock();
            try {
                partitionSet.forEach(offsetRangeCache::remove);
            } finally {
                writeLock.unlock();
            }
        }
    }


    static class OffsetRangeCacheRecord {
        private final OffsetDateTime expirationDate;
        private final OffsetRange offsetRange;

        public OffsetRangeCacheRecord(OffsetRange offsetRange) {
            this.expirationDate = OffsetDateTime.now().plusMinutes(EXPIRATION_TIME_MIN);
            this.offsetRange = offsetRange;
        }

        public OffsetDateTime getExpirationDate() {
            return expirationDate;
        }

        public OffsetRange getOffsetRange() {
            return offsetRange;
        }

    }


    public static class TopicOffsetRangeCacheCleaner extends TimerTask {
        private static final String THREAD_NAME = "TopicOffsetRangeCacheCleaner";
        private TopicOffsetRangeCacheCleaner(Long logIntervalMin) {
            long logIntervalMs = logIntervalMin * 60 * 1000;
            new Timer().schedule(this, logIntervalMs, logIntervalMs);
        }
        public static TopicOffsetRangeCacheCleaner with(Long logIntervalMin) {
            return new TopicOffsetRangeCacheCleaner(logIntervalMin);
        }
        @Override
        public void run() {
            Thread.currentThread().setName(THREAD_NAME);
            clean();
        }
        private void clean() {
            Set<TopicPartition> partitionSet = new HashSet<>();
            offsetRangeCache.forEach((topicPartition, cacheRecord) -> {
                if(now().isAfter(cacheRecord.getExpirationDate())) {
                    partitionSet.add(topicPartition);
                }
            });
            removeRecords(partitionSet);

        }
    }


}
