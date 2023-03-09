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
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * @author Mikhail_Vershkov
 */
public class CachedTopicRecordFetcherTest {

    private final static String TOPIC_NAME = "test-topic";
    private final static int PARTITIONS = 10;
    private final static int RECORDS_IN_PARTITION = 10;
    private final static int SHIFT_OFFSET_BY_PARTITION = 4;
    private final static int LIMIT = 30;


    private final RecordBiDirectionalFetcher<String,String> cachedTopicRecordFetcher =
            CachedTopicRecordFetcher.with(new HashMap<>());

    @BeforeEach
    public void before() {
        populateCache();
    }

    @Test
    public void doFetchByOffsetsForwardTest() {
        Map<TopicPartition, Long> offsets = generateOffsets();
        RecordFetchResult<String, String> result = cachedTopicRecordFetcher.fetchByOffsets(
                offsets, LIMIT,null, 100L,
                BiDirectionalTopicRecordFetcher.FetchDirection.FORWARD);
        Assertions.assertEquals(LIMIT,result.count());
        offsets.forEach((key, value) -> {
            Assertions.assertEquals(result.getPerPartitionResult(key).getScannedOffsets().getSmallest(),
                    value + 1);
            Assertions.assertEquals(result.getPerPartitionResult(key).getScannedOffsets().getLargest(),
                    value + LIMIT/RECORDS_IN_PARTITION);
            Assertions.assertEquals(result.getPerPartitionResult(key).getPartitionOffsets().getSmallest(),
                    (long) key.partition() * RECORDS_IN_PARTITION);
            Assertions.assertEquals(result.getPerPartitionResult(key).getPartitionOffsets().getLargest(),
                    (long) (key.partition() + 1) *RECORDS_IN_PARTITION-1);
        });
    }

    @Test
    public void doFetchByOffsetsBackwardTest() {
        Map<TopicPartition, Long> offsets = generateOffsets();
        RecordFetchResult<String, String> result = cachedTopicRecordFetcher.fetchByOffsets(
                offsets, LIMIT,null, 100L,
                BiDirectionalTopicRecordFetcher.FetchDirection.BACKWARD);
        Assertions.assertEquals(LIMIT,result.count());
        offsets.forEach((key, value) -> {
            Assertions.assertEquals(result.getPerPartitionResult(key).getScannedOffsets().getSmallest(),
                    value - LIMIT/RECORDS_IN_PARTITION);
            Assertions.assertEquals(result.getPerPartitionResult(key).getScannedOffsets().getLargest(), value - 1 );
            Assertions.assertEquals(result.getPerPartitionResult(key).getPartitionOffsets().getSmallest(),
                    (long) key.partition() * RECORDS_IN_PARTITION);
            Assertions.assertEquals(result.getPerPartitionResult(key).getPartitionOffsets().getLargest(),
                    (long) (key.partition() + 1) *RECORDS_IN_PARTITION-1);
        });
    }


    private static Map<TopicPartition, Long> generateOffsets() {
        Map<TopicPartition, Long> map = new HashMap<>();
        IntStream.range(0, PARTITIONS).forEach(partition ->
                map.put(new TopicPartition(TOPIC_NAME, partition),
                        (long)partition * RECORDS_IN_PARTITION + SHIFT_OFFSET_BY_PARTITION));
        return map;
    }

    private static void populateCache() {
        IntStream.range(0, PARTITIONS).forEach(partition ->
                CachedTopicRecordFetcher.addRecordToCache(
                    new TopicPartition(TOPIC_NAME, partition),
                    new CachedTopicRecordFetcher.PartitionRecordsCache<>(
                            OffsetDateTime.now().plusMinutes(60),
                            IntStream.range(0, RECORDS_IN_PARTITION)
                                  .mapToObj( recordCount ->
                                          buildConsumerRecord(partition,
                                                  partition * CachedTopicRecordFetcherTest.RECORDS_IN_PARTITION + recordCount))
                            .collect(Collectors.toList())
                    )
                )
        );
    }
    private static ConsumerRecord<String,String> buildConsumerRecord(int partition, int offset) {
        return new ConsumerRecord<>(TOPIC_NAME, partition, offset, "key-" + offset, "value: " + offset);
    }


}
