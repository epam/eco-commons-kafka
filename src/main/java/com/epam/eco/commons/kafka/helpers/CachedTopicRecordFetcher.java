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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import com.epam.eco.commons.kafka.KafkaUtils;
import com.epam.eco.commons.kafka.OffsetRange;
import static java.time.OffsetDateTime.now;
import static java.util.Objects.isNull;

/**
 * @author Mikhail_Vershkov
 */
public class CachedTopicRecordFetcher<K, V> extends BiDirectionalTopicRecordFetcher<K,V> implements RecordBiDirectionalFetcher<K,V> {

    private final static Map<TopicPartition, PartitionRecordsCache> recordsCache = new ConcurrentHashMap<>();
    private final static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final static Lock readLock = lock.readLock();
    private final static Lock writeLock = lock.writeLock();
    private static final long expirationTimeMin = 60L;

    private CachedTopicRecordFetcher(String bootstrapServers,
                                     Map<String, Object> consumerConfig ) {
        super(bootstrapServers, consumerConfig);
    }

    public static <K, V> CachedTopicRecordFetcher<K, V> with(Map<String, Object> consumerConfig) {
        return new CachedTopicRecordFetcher<>(null, consumerConfig);
    }

    public static <K, V> CachedTopicRecordFetcher<K, V> with(String bootstrapServers) {
        return new CachedTopicRecordFetcher<>(bootstrapServers, null);
    }

    @Override
    public RecordFetchResult<K, V> fetchByOffsets(Map<TopicPartition, Long> offsets,
                                                  long limit,
                                                  Predicate<ConsumerRecord<K, V>> filter,
                                                  long timeoutInMs,
                                                  FetchDirection fetchDirection) {

        populateCacheForAllPartitions(offsets, timeoutInMs);

        Map<TopicPartition, Long> limitsPerPartition = calculateLimitsByPartition(offsets.keySet(), limit);

        Map<TopicPartition, PartitionRecordFetchResult<K, V>> resultMap = new HashMap<>();
        offsets.keySet().forEach(topicPartition ->  resultMap.put(topicPartition,
                buildFetchResult(topicPartition, offsets.get(topicPartition), limitsPerPartition.get(topicPartition), fetchDirection))
        );

        return new RecordFetchResult<>(resultMap);
    }

    @Override
    public RecordFetchResult<K, V> fetchByTimestamps(Map<TopicPartition, Long> timestamps,
                                                     long limit, Predicate<ConsumerRecord<K, V>> filter,
                                                     long timeoutInMs,
                                                     FetchDirection direction) {

        Map<TopicPartition, Long> limitsPerPartition = calculateLimitsByPartition(timestamps.keySet(), limit);

        populateCacheForAllPartitions(timestamps, timeoutInMs);

        Map<TopicPartition, PartitionRecordFetchResult<K, V>> resultMap = new HashMap<>();
        timestamps.keySet().forEach(topicPartition ->  resultMap.put(topicPartition,
                buildFetchResultByTimestamp(topicPartition, timestamps.get(topicPartition), limitsPerPartition.get(topicPartition), direction))
        );

        return new RecordFetchResult<>(resultMap);
    }

    private PartitionRecordFetchResult<K, V> buildFetchResult(TopicPartition topicPartition,
                                                              Long offset,
                                                              Long limit,
                                                              FetchDirection fetchDirection) {
        PartitionRecordFetchResult<K, V> partitionRecordFetchResult;
        try {
            readLock.lock();
            List<ConsumerRecord<K,V>> records = recordsCache.get(topicPartition)
                    .getRecordsByOffsetAndLimit(offset, limit, fetchDirection);
            OffsetRange allRecordsOffsetRange = getAllRecordsOffsetRange(recordsCache.get(topicPartition).getRecords());
            partitionRecordFetchResult = new PartitionRecordFetchResult<>(topicPartition, records, allRecordsOffsetRange,
                    getFetchedRecordsOffsetRange(records, getFetchedBound(fetchDirection, allRecordsOffsetRange)) );
        } finally {
            readLock.unlock();
        }
        return partitionRecordFetchResult;
    }

    private PartitionRecordFetchResult<K, V> buildFetchResultByTimestamp(TopicPartition topicPartition,
                                                                         Long timestamp,
                                                                         Long limit,
                                                                         FetchDirection fetchDirection) {
        PartitionRecordFetchResult<K, V> partitionRecordFetchResult;
        try {
            readLock.lock();
            List<ConsumerRecord<K, V>> records = recordsCache.get(topicPartition).getRecordsByTimestamp(timestamp, limit, fetchDirection);
            OffsetRange allRecordsOffsetRange = getAllRecordsOffsetRange(recordsCache.get(topicPartition).getRecords());
            partitionRecordFetchResult = new PartitionRecordFetchResult<>(topicPartition, records, allRecordsOffsetRange,
                    getFetchedRecordsOffsetRange(records, getFetchedBound(fetchDirection, allRecordsOffsetRange)) );
        } finally {
            readLock.unlock();
        }
        return partitionRecordFetchResult;
    }

    private long getFetchedBound(FetchDirection direction, OffsetRange allRecordsOffsetRange) {
        return direction == FetchDirection.FORWARD ? allRecordsOffsetRange.getLargest() : allRecordsOffsetRange.getSmallest();
    }

    private OffsetRange getFetchedRecordsOffsetRange(List<ConsumerRecord<K, V>> fetchedRecords, long bound) {
        if(fetchedRecords.isEmpty()) {
            return OffsetRange.with(bound, false,bound, false);
        }
        return OffsetRange.with(fetchedRecords.get(0).offset(), true,
                fetchedRecords.get(fetchedRecords.size() - 1).offset(), true);
    }
    private OffsetRange getAllRecordsOffsetRange(List<ConsumerRecord<K, V>> records) {
        if(records.isEmpty()) {
            return OffsetRange.with(0L, false, 0L, false);
        }
        return OffsetRange.with(records.get(0).offset(), false, records.get(records.size() - 1).offset(), true);
    }


    private void populateCacheForAllPartitions(Map<TopicPartition, Long> offsets, long timeoutInMs) {
        offsets.keySet().forEach(topicPartition -> {
            PartitionRecordsCache<K,V> partitionRecordsCache = recordsCache.get(topicPartition);
            if(isNull(partitionRecordsCache) || OffsetDateTime.now().isAfter(partitionRecordsCache.getExpirationTime())) {
                recordsCache.remove(topicPartition);
                populateCacheByPartition(topicPartition, timeoutInMs);
            }
        });
    }

    private void populateCacheByPartition(TopicPartition topicPartition, long timeoutMs) {
        try {
            writeLock.lock();
            recordsCache.put(topicPartition,
                    new PartitionRecordsCache<>(OffsetDateTime.now().plusMinutes(expirationTimeMin),
                            fetchAllByTopicPartition(topicPartition, timeoutMs)));
        } finally {
            writeLock.unlock();
        }
    }

    private List<ConsumerRecord<K, V>> fetchAllByTopicPartition(TopicPartition topicPartition, long timeoutMs) {
        try (KafkaConsumer<K, V> consumer = new KafkaConsumer<>(consumerConfig)) {
            return doFetchAllPartitionRecords(consumer, topicPartition, timeoutMs);
        }

    }

    private List<ConsumerRecord<K, V>> doFetchAllPartitionRecords(KafkaConsumer<K, V> consumer, TopicPartition partition, long timeoutMs) {

        List<TopicPartition> partitions = List.of(partition);
        Map<TopicPartition, OffsetRange> offsetRanges = fetchOffsetRanges(partitions);

        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);

        long fetchStart = System.currentTimeMillis();
        List<ConsumerRecord<K, V>> consumerRecords = new ArrayList<>();
        while(true) {
            ConsumerRecords<K, V> records = consumer.poll(POLL_TIMEOUT);

            if(records.count() > 0) {
                Map<TopicPartition, Long> consumedOffsets = new HashMap<>(KafkaUtils.getConsumerPositions(consumer));
                consumerRecords.addAll(records.records(partition));
                if(areAllOffsetsReachedEndOfRange(consumedOffsets, offsetRanges)) {
                    break;
                }
            }
            if(System.currentTimeMillis() - fetchStart > timeoutMs) {
                consumerRecords.addAll(records.records(partition));
                break;
            }
        }
        return consumerRecords;
    }

    protected boolean areAllOffsetsReachedEndOfRange(
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

    protected static <K,V> void addRecordToCache(TopicPartition topicPartition,
                                                 PartitionRecordsCache<K,V> recordCache) {
         recordsCache.put(topicPartition, recordCache);
    }

    static class PartitionRecordsCache<K, V> {
        private final OffsetDateTime expirationTime;
        private final List<ConsumerRecord<K, V>> records;
        private final long smallest;
        private final  long largest;

        public PartitionRecordsCache(OffsetDateTime expirationTime, List<ConsumerRecord<K, V>> records) {
            this.expirationTime = expirationTime;
            this.records = records;
            this.smallest = records.size()>0 ? records.get(0).offset() : 0L;
            this.largest = records.size()>0 ? records.get(records.size()-1).offset() : 0L;
        }

        public OffsetDateTime getExpirationTime() {
            return expirationTime;
        }

        public List<ConsumerRecord<K, V>> getRecords() {
            return records;
        }

        public long getSmallest() {
            return smallest;
        }

        public long getLargest() {
            return largest;
        }

        public List<ConsumerRecord<K, V>> getRecordsByOffsetAndLimit(long offset, Long limit, FetchDirection fetchDirection) {
            List<ConsumerRecord<K, V>> rangedRecords = new ArrayList<>();
            if(isNull(limit)) {
                return rangedRecords;
            }
            int count = 0;
            for(ConsumerRecord<K, V> record : records) {
                if(fetchDirection == FetchDirection.FORWARD) {
                    if(record.offset() > offset) {
                        if(++ count > limit) {
                            break;
                        }
                        rangedRecords.add(record);
                    }
                } else {
                    if(record.offset() < offset) {
                        count++;
                    }
                    rangedRecords = records.subList(count - limit < 0 ? 0 : count - limit.intValue(), count);
                }
            }
            return rangedRecords;
        }


        public List<ConsumerRecord<K, V>> getRecordsByTimestamp(long timestamp, long limit, FetchDirection direction) {
            List<ConsumerRecord<K, V>> rangedRecords = new ArrayList<>();
            int count = 0;
            for(ConsumerRecord<K, V> record : records) {
                if(direction == FetchDirection.FORWARD) {
                    if(record.timestamp() > timestamp) {
                        if(++ count > limit) {
                            break;
                        }
                        rangedRecords.add(record);
                    }
                } else if(direction == FetchDirection.BACKWARD) {
                    if(record.timestamp() < timestamp) {
                        count++;
                    }
                    rangedRecords = records.subList(count - limit < 0 ? 0 : count - (int) limit, count);
                }
            }
            return rangedRecords;
        }

    }
    public static class TopicCacheCleaner extends TimerTask {
        private static final String THREAD_NAME = "TopicCacheCleaner";
        public TopicCacheCleaner(Long logIntervalMin) {
            long logIntervalMs = logIntervalMin * 60 * 1000;
            new Timer().schedule(this, logIntervalMs, logIntervalMs);
        }
        public static TopicCacheCleaner with(Long logIntervalMin) {
            return new TopicCacheCleaner(logIntervalMin);
        }
        @Override
        public void run() {
            Thread.currentThread().setName(THREAD_NAME);
            clean();
        }
        private void clean() {
            Set<TopicPartition> partitionSet = new HashSet<>();
            recordsCache.forEach((topicPartition, cache) -> {
                if(now().isAfter(cache.getExpirationTime())) {
                    partitionSet.add(topicPartition);
                }
            });
            if(! partitionSet.isEmpty()) {
                writeLock.lock();
                try {
                    partitionSet.forEach(recordsCache::remove);
                } finally {
                    writeLock.unlock();
                }
            }
        }
    }

}
