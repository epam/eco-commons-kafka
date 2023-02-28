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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;

import com.epam.eco.commons.kafka.KafkaUtils;
import com.epam.eco.commons.kafka.OffsetRange;
import com.epam.eco.commons.kafka.TopicPartitionComparator;

/**
 * @author Mikhail_Vershkov
 */

public class BiDirectionalTopicRecordFetcher<K,V> extends TopicRecordFetcher<K,V> implements RecordBiDirectionalFetcher<K,V> {

    protected BiDirectionalTopicRecordFetcher(String bootstrapServers, Map<String, Object> consumerConfig) {
        super(bootstrapServers, consumerConfig);
    }

    public static <K, V> BiDirectionalTopicRecordFetcher<K, V> with(Map<String, Object> consumerConfig) {
        return new BiDirectionalTopicRecordFetcher<>(null, consumerConfig);
    }

    public static <K, V> BiDirectionalTopicRecordFetcher<K, V> with(String bootstrapServers) {
        return new BiDirectionalTopicRecordFetcher<>(bootstrapServers, null);
    }

    @Override
    public RecordFetchResult<K, V> fetchByOffsets(Map<TopicPartition, Long> offsets,
                                                  long limit,
                                                  Predicate<ConsumerRecord<K, V>> filter,
                                                  long timeoutInMs,
                                                  FetchDirection direction) {
        validateOffsets(offsets);
        Validate.isTrue(limit > 0, "Limit is invalid");
        Validate.isTrue(timeoutInMs > 0, "Timeout is invalid");

        try (KafkaConsumer<K, V> consumer = new KafkaConsumer<>(consumerConfig)) {
            return direction == FetchDirection.BACKWARD ?
                    doReverseFetchByOffsets(consumer, offsets, limit, filter, timeoutInMs) :
                    doFetchByOffsets(consumer, offsets, limit, filter, timeoutInMs);
        }
    }

    @Override
    public RecordFetchResult<K, V> fetchByTimestamps(Map<TopicPartition, Long> partitionTimestamps,
                                                     long limit,
                                                     Predicate<ConsumerRecord<K, V>> filter,
                                                     long timeoutInMs,
                                                     FetchDirection direction) {
        validatePartitionTimestamps(partitionTimestamps);
        Validate.isTrue(limit > 0, "Limit is invalid");
        Validate.isTrue(timeoutInMs > 0, "Timeout is invalid");

        try (KafkaConsumer<K, V> consumer = new KafkaConsumer<>(consumerConfig)) {
            return doFetchByTimestamps(consumer, partitionTimestamps, limit, filter, timeoutInMs, direction);
        }
    }


    private RecordFetchResult<K, V> doFetchByTimestamps(KafkaConsumer<K, V> consumer, Map<TopicPartition, Long> partitionTimestamps, long limit, Predicate<ConsumerRecord<K, V>> filter, long timeoutMs, FetchDirection direction) {
        Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes = consumer.offsetsForTimes(partitionTimestamps);

        Map<TopicPartition, Long> offsets = offsetsForTimes.entrySet().stream().filter(entry -> entry.getValue() != null).collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().offset()));

        return direction == FetchDirection.FORWARD  ? doFetchByOffsets(consumer, offsets, limit, filter, timeoutMs) : doReverseFetchByOffsets(consumer, offsets, limit, filter, timeoutMs);
    }



    protected RecordFetchResult<K, V> doReverseFetchByOffsets(KafkaConsumer<K, V> consumer,
                                                            Map<TopicPartition, Long> offsets,
                                                            long limit,
                                                            Predicate<ConsumerRecord<K, V>> filter,
                                                            long timeoutMs) {
        if(offsets.isEmpty()) {
            return RecordFetchResult.emptyResult();
        }

        Map<TopicPartition, OffsetRange> offsetRanges = fetchOffsetRanges(offsets.keySet());

        offsets = filterOutUselessOffsetsReverseFetch(offsets, offsetRanges);
        if (offsets.isEmpty()) {
            return RecordFetchResult.emptyResult();
        }

        checkAndCorrectBounds(offsets, offsetRanges);

        Map<TopicPartition, BiDirectionalRecordCollector> collectors =
                initBiDirectionalRecordCollectorsForPartitions(offsets.keySet(), filter, limit);

        assignConsumerToPartitions(consumer, offsets);

        long fetchStart = System.currentTimeMillis();

        Map<TopicPartition, OffsetRange> currentChunkOffsetRanges = 
                getFirstChunkRanges(offsets, offsetRanges, collectors);

        Map<TopicPartition, Long> consumedOffsets = new HashMap<>();

        while(true) {

            seekOffsetsOfCurrentChunkBeginning(consumer, currentChunkOffsetRanges);
            collectRecordsFromCurrentChunk(consumer, timeoutMs, collectors, fetchStart,
                                           currentChunkOffsetRanges, consumedOffsets);

            getNextChunkRange(currentChunkOffsetRanges, offsetRanges, collectors);

            if( allCurrentChunksIsEmpty(currentChunkOffsetRanges) ||
                areAllOffsetsReachedEndOfRange(consumedOffsets, offsetRanges) ||
                areAllCollectorsDone(collectors) ||
                isTimeExpired(fetchStart, timeoutMs)) {
                break;
            }

        }
        collectors.values().forEach(BiDirectionalRecordCollector::sortByOffsets);

        return toFetchResult(collectors, offsetRanges);
    }

    private boolean allCurrentChunksIsEmpty(Map<TopicPartition,OffsetRange> currentChunkOffsetRanges) {
        for(OffsetRange currentChunkOffsetRange: currentChunkOffsetRanges.values()) {
            if(currentChunkOffsetRange.getSize()>0) {
                return false;
            }
        }
        return true;
    }

    protected Map<TopicPartition, Long> filterOutUselessOffsetsReverseFetch(Map<TopicPartition, Long> offsets,
                                                                            Map<TopicPartition, OffsetRange> offsetRanges) {
        return offsets.entrySet().stream().filter(entry -> {
            OffsetRange range = offsetRanges.get(entry.getKey());
            Long offset = entry.getValue();
            return range != null && range.getSize() > 0 && range.contains(offset);
        }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    protected Map<TopicPartition, BiDirectionalRecordCollector> initBiDirectionalRecordCollectorsForPartitions(
                                                                    Collection<TopicPartition> partitions,
                                                                    Predicate<ConsumerRecord<K, V>> filter,
                                                                    long limit) {
        if(partitions.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<TopicPartition, BiDirectionalRecordCollector> collectors = new TreeMap<>(TopicPartitionComparator.INSTANCE);
        Map<TopicPartition, Long> limitsPerPartition = calculateLimitsByPartition(partitions, limit);
        limitsPerPartition.forEach((key, value) -> collectors.put(key, new BiDirectionalRecordCollector(filter, value)));

        return collectors;
    }

    private void collectRecordsFromCurrentChunk(KafkaConsumer<K, V> consumer,
                                                long timeoutMs,
                                                Map<TopicPartition, BiDirectionalRecordCollector> collectors,
                                                long fetchStart,
                                                Map<TopicPartition, OffsetRange> currentChunkOffsets,
                                                Map<TopicPartition, Long> consumedOffsets) {
        while(true) {
            ConsumerRecords<K, V> records = consumer.poll(POLL_TIMEOUT);

            if(records.count() > 0) {

                collectLastLimitRecords(records, collectors, currentChunkOffsets);
                consumedOffsets.putAll(KafkaUtils.getConsumerPositions(consumer));

                if( areAllCollectorsDone(collectors) ||
                    areAllOffsetsReachedEndOfRange(consumedOffsets, currentChunkOffsets) ||
                    isTimeExpired(fetchStart, timeoutMs)) {
                    break;
                }
            }

            if(isTimeExpired(fetchStart,timeoutMs)) {
                break;
            }
        }

    }

    private Map<TopicPartition, OffsetRange> getFirstChunkRanges(Map<TopicPartition, Long> currentChunkOffsets,
                                                                 Map<TopicPartition, OffsetRange> offsetRanges,
                                                                 Map<TopicPartition, BiDirectionalRecordCollector> collectors) {
        Map<TopicPartition, OffsetRange> result = new HashMap<>();
        currentChunkOffsets.forEach((topicPartition, largestOffset) -> {
            long lowerBound = offsetRanges.get(topicPartition).getSmallest();
            long startOffset = Math.max(largestOffset - collectors.get(topicPartition).getLimit() + 1, lowerBound);
            result.put(topicPartition, OffsetRange.with(startOffset, true, largestOffset, true));
        });
        return result;
    }

    private void getNextChunkRange(Map<TopicPartition, OffsetRange> currentChunkOffsets,
                                   Map<TopicPartition, OffsetRange> offsetRanges,
                                   Map<TopicPartition, BiDirectionalRecordCollector> collectors) {
        currentChunkOffsets.keySet().forEach(topicPartition -> {

            long largestOffset = currentChunkOffsets.get(topicPartition).getSmallest();
            boolean smallestOffsetInclusive = true;
            boolean largestOffsetInclusive = true;

            long lowerBound = offsetRanges.get(topicPartition).getSmallest();
            long smallestOffset = largestOffset - collectors.get(topicPartition).getLimit();
            if(smallestOffset < lowerBound) {
                smallestOffset = lowerBound;
                smallestOffsetInclusive=false;
            }

            if(largestOffset <= offsetRanges.get(topicPartition).getSmallest()) {
                largestOffset = offsetRanges.get(topicPartition).getSmallest();
                largestOffsetInclusive = false;
                smallestOffset = lowerBound;
                smallestOffsetInclusive=false;
            }

            currentChunkOffsets.put(topicPartition,
                    OffsetRange.with(smallestOffset, smallestOffsetInclusive, largestOffset, largestOffsetInclusive));
        });
    }

    private void assignConsumerToPartitions(Consumer<K, V> consumer, Map<TopicPartition, Long> offsets) {
        List<TopicPartition> partitions = new ArrayList<>(offsets.keySet());
        consumer.assign(partitions);
    }

    private void seekOffsetsOfCurrentChunkBeginning(KafkaConsumer<K, V> consumer,
                                                    Map<TopicPartition, OffsetRange> offsets) {
        offsets.forEach((partition, offsetRange)-> consumer.seek(partition,offsetRange.getSmallest()));;
    }


    public boolean collectLastLimitRecords(ConsumerRecords<K, V> records,
                                            Map<TopicPartition, BiDirectionalRecordCollector> collectors,
                                            Map<TopicPartition, OffsetRange> currentChunkOffsets) {
        boolean anythingCollected = false;
        for(Map.Entry<TopicPartition, BiDirectionalRecordCollector> entry : collectors.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            if(currentChunkOffsets.get(topicPartition).getSize()>0) {
                BiDirectionalRecordCollector biDirectionalCollector = entry.getValue();
                for(ConsumerRecord<K, V> record : reversedMessages(records.records(topicPartition))) {
                    if(record.offset() > currentChunkOffsets.get(topicPartition).getLargest() ||
                            record.offset() < currentChunkOffsets.get(topicPartition).getSmallest()) {
                        continue;
                    }
                    if(biDirectionalCollector.isLimitReached()) {
                        break;
                    }
                    biDirectionalCollector.add(record);
                    anythingCollected = true;
                }
            }

        }
        return anythingCollected;
    }

    protected boolean areAllOffsetsReachedEndOfRange(
            Map<TopicPartition, Long> offsets,
            Map<TopicPartition, OffsetRange> offsetRanges) {
        if(offsets.isEmpty()) {
            return false;
        }
        for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet()) {
            Long offset = entry.getValue();
            OffsetRange range = offsetRanges.get(entry.getKey());
            if (range.getSmallest()>range.getLargest() || range.contains(offset)) {
                return false;
            }
        }
        return true;
    }

    private List<ConsumerRecord<K, V>> reversedMessages(List<ConsumerRecord<K, V>> messages) {
        return messages.stream()
                .sorted(Comparator.comparing(ConsumerRecord::offset, Comparator.reverseOrder()))
                .collect(Collectors.toList());
    }

    public enum FetchDirection {
        FORWARD, BACKWARD
    }

    class BiDirectionalRecordCollector extends RecordCollector {

        public BiDirectionalRecordCollector(Predicate<ConsumerRecord<K, V>> filter, long limit) {
            super(filter, limit);
        }
        public void add(ConsumerRecord<K, V> record) {
            if(isRecordExists(record)) {
                return;
            }
            if(isLimitReached()) {
                return;
            }

            update(record);

            if(passesFilter(record)) {
                records.add(record);
            }
        }

        private boolean isRecordExists(ConsumerRecord<K, V> record) {
            return records.stream()
                    .anyMatch(testRecord -> testRecord.offset() == record.offset() &&
                            testRecord.partition() == record.partition());
        }
        protected void sortByOffsets() {
            records.sort(Comparator.comparingLong(ConsumerRecord::offset));
        }

        private void update(ConsumerRecord<K, V> record) {
            if(largestScannedOffset == - 1 ||
               record.offset() > largestScannedOffset) {
                largestScannedOffset = record.offset();
            }
            smallestScannedOffset = record.offset();
        }
        public long getSmallestScannedOffset() {
            return records.stream().map(ConsumerRecord::offset)
                    .min(Comparator.naturalOrder())
                    .orElse(this.smallestScannedOffset);
        }
        public long getLargestScannedOffset() {
            return records.stream().map(ConsumerRecord::offset)
                    .max(Comparator.naturalOrder())
                    .orElse(this.largestScannedOffset);

        }

    }

}
