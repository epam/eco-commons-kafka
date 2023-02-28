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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.epam.eco.commons.kafka.KafkaUtils;
import com.epam.eco.commons.kafka.OffsetRange;

import static java.time.OffsetDateTime.now;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.Validate.notNull;

/**
 * @author Andrei_Tytsik
 */
public class CachedTopicOffsetRangeFetcher extends TopicOffsetRangeFetcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(CachedTopicOffsetRangeFetcher.class);
    private static final long EXPIRATION_TIME_MIN = 10L;
    private static final Map<TopicPartition, OffsetRangeCacheRecord> offsetRangeCache = new ConcurrentHashMap<>();
    private final static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final static Lock readLock = lock.readLock();
    private final static Lock writeLock = lock.writeLock();

    private CachedTopicOffsetRangeFetcher(String bootstrapServers, Map<String, Object> consumerConfig) {
        super(bootstrapServers,consumerConfig);
    }

    public static CachedTopicOffsetRangeFetcher with(Map<String, Object> consumerConfig) {
        return new CachedTopicOffsetRangeFetcher(null, consumerConfig);
    }

    public static CachedTopicOffsetRangeFetcher with(String bootstrapServers) {
        return new CachedTopicOffsetRangeFetcher(bootstrapServers, null);
    }


    public Map<TopicPartition, OffsetRange> fetchForPartitions(Collection<TopicPartition> partitions) {
        Validate.notEmpty(partitions, "Collection of partitions is null or empty");
        Validate.noNullElements(partitions, "Collection of partitions contains null elements");

        try (KafkaConsumer<?, ?> consumer = new KafkaConsumer<>(consumerConfig)) {
            return calculateIfAbsent(consumer, partitions);
        }
    }

    public Map<TopicPartition, OffsetRange> fetchForTopics(String ... topicNames) {
        return fetchForTopics(
                topicNames != null ? Arrays.asList(topicNames) : null);
    }

    public Map<TopicPartition, OffsetRange> fetchForTopics(Collection<String> topicNames) {
        Validate.notEmpty(topicNames, "Collection of topic names is null or empty");
        Validate.noNullElements(topicNames, "Collection of topic names contains null elements");

        try (KafkaConsumer<?, ?> consumer = new KafkaConsumer<>(consumerConfig)) {
            List<TopicPartition> partitions =
                    KafkaUtils.getTopicPartitionsAsList(consumer, topicNames);
            return calculateIfAbsent(consumer, partitions);
        }
    }

    Map<TopicPartition, OffsetRange> calculateIfAbsent(Consumer<?,?> consumer,
                                                       Collection<TopicPartition> partitions) {

        Map<TopicPartition, OffsetRange> results = partitions.stream()
                .filter(topicPartition->nonNull(getCacheValue(topicPartition)))
                .collect(Collectors.toMap( Function.identity(), CachedTopicOffsetRangeFetcher::getCacheValue));

        List<TopicPartition> nonCachedPartitions = partitions.stream()
                .filter(topicPartition->isNull(getCacheValue(topicPartition)))
                .collect(Collectors.toList());

        Map<TopicPartition, OffsetRange> calculatedValues = doFetch(consumer, nonCachedPartitions);
        calculatedValues.forEach(CachedTopicOffsetRangeFetcher::putCacheValue);
        results.putAll(calculatedValues);

        return results;
    }

    public static OffsetRange getCacheValue(TopicPartition topicPartition) {
        readLock.lock();
        OffsetRange offsetRange = null;
        try {
            OffsetRangeCacheRecord record = offsetRangeCache.get(topicPartition);
            if(nonNull(record) && OffsetDateTime.now().isBefore(record.getExpirationDate())) {
                offsetRange=record.getOffsetRange();
            }
        }
        finally {
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


    static class OffsetRangeCacheRecord {
        private OffsetDateTime expirationDate;
        private OffsetRange offsetRange;

        public OffsetRangeCacheRecord(OffsetRange offsetRange) {
            this.expirationDate = OffsetDateTime.now().plusMinutes(EXPIRATION_TIME_MIN);
            this.offsetRange = offsetRange;
        }

        public OffsetDateTime getExpirationDate() {
            return expirationDate;
        }

        public void setExpirationDate(OffsetDateTime expirationDate) {
            this.expirationDate = expirationDate;
        }

        public OffsetRange getOffsetRange() {
            return offsetRange;
        }

        public void setOffsetRange(OffsetRange offsetRange) {
            this.offsetRange = offsetRange;
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
            if(! partitionSet.isEmpty()) {
                writeLock.lock();
                try {
                    partitionSet.forEach(offsetRangeCache::remove);
                } finally {
                    writeLock.unlock();
                }
            }
        }
    }


}
