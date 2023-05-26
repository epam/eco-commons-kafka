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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.LongStream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import com.epam.eco.commons.kafka.OffsetRange;

import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

/**
 * @author Mikhail_Vershkov
 */

@ExtendWith(MockitoExtension.class)
public class BiDirectionalTopicRecordFetcherTest {
    private final static long[] STARTING_OFFSETS = {5, 10, 10, 10, 10};
    private final static long[] REAL_POLL_BEGINNING_POSITION = {5, 4, 4, 5, 7};
    private final static OffsetRange[] OFFSET_RANGES = {
            new OffsetRange(5, true, 20, true),
            new OffsetRange(4, true, 20, true),
            new OffsetRange(4, true, 20, true),
            new OffsetRange(5, true, 20, true),
            new OffsetRange(7, true, 20, true)};

    private final static long[][] EXPECTED_OFFSETS = {
            {5},
            {10, 9, 8, 7, 6},
            {10, 9, 8, 7, 6},
            {10, 9, 8, 7, 6},
            {10, 9, 8, 7}};
    private final static long[][] EXPECTED_OFFSETS_LESS = {
            {5},
            {10, 9, 8, 7, 6},
            {10, 9, 8, 7, 6},
            {10, 9, 8, 7, 6},
            {10, 9, 8}};
    private final static int EXPECTED_RESULT_SIZE = 20;

    private final static String TOPIC_NAME = "test-topic";
    private final static int LIMIT = 25;
    private final static long TIMEOUT_MS = 1000000L;

    private final BiDirectionalTopicRecordFetcher<String, String> biDirectionalTopicRecordFetcher =
            BiDirectionalTopicRecordFetcher.with(new HashMap<>());

    private final BiDirectionalTopicRecordFetcher<String, String> biDirectionalTopicRecordFetcherSpy =
            spy(biDirectionalTopicRecordFetcher);

    private final Map<TopicPartition, Long> offsets =
            ConfluentUtils.generateStartingOffsets(TOPIC_NAME,STARTING_OFFSETS);


    @Test
    public void doReverseFetchByOffsetsTest() {

        ConsumerMock consumerMock = ConsumerMock.builder()
                .topicName(TOPIC_NAME)
                .offsetRanges(OFFSET_RANGES)
                .realPollBeginningPosition(REAL_POLL_BEGINNING_POSITION)
                .startingOffsets(STARTING_OFFSETS)
                .build();
        consumerMock.before();

        doReturn(ConfluentUtils.generateOffsetRanges(consumerMock.getTopicName(),
                consumerMock.getStartingOffsets(),consumerMock.getOffsetRanges()))
                .when(biDirectionalTopicRecordFetcherSpy).fetchOffsetRanges(anyCollection());

        RecordFetchResult<String, String> results = biDirectionalTopicRecordFetcherSpy
                .doReverseFetchByOffsets(consumerMock.getConsumer(), offsets, LIMIT, null, TIMEOUT_MS);

        Assertions.assertEquals(EXPECTED_RESULT_SIZE, results.getRecords().size());
        results.getResults().forEach((topicPartition, partitionRecordFetchResult) ->
                Assertions.assertArrayEquals(Arrays.stream(EXPECTED_OFFSETS[topicPartition.partition()]).sorted().toArray(),
                        partitionRecordFetchResult.getRecords().stream().mapToLong(ConsumerRecord::offset).sorted().toArray()));
    }

    @Test
    public void doReverseFetchByOffsetsLessLimitTest() {
        REAL_POLL_BEGINNING_POSITION[4] = 8L;
        ConsumerMock consumerMock = ConsumerMock.builder()
                .topicName(TOPIC_NAME)
                .offsetRanges(OFFSET_RANGES)
                .realPollBeginningPosition(REAL_POLL_BEGINNING_POSITION)
                .startingOffsets(STARTING_OFFSETS)
                .build();
        consumerMock.before();

        doReturn(ConfluentUtils.generateOffsetRanges(consumerMock.getTopicName(),
                consumerMock.getStartingOffsets(),consumerMock.getOffsetRanges()))
                .when(biDirectionalTopicRecordFetcherSpy).fetchOffsetRanges(anyCollection());

        RecordFetchResult<String, String> results = biDirectionalTopicRecordFetcherSpy
                .doReverseFetchByOffsets(consumerMock.getConsumer(),
                        ConfluentUtils.generateStartingOffsets(consumerMock.getTopicName(),
                                consumerMock.getStartingOffsets()),
                        25, null, TIMEOUT_MS);

        Assertions.assertEquals(19, results.getRecords().size());
        results.getResults().forEach((topicPartition, partitionRecordFetchResult) ->
                Assertions.assertArrayEquals(Arrays.stream(EXPECTED_OFFSETS_LESS[topicPartition.partition()]).sorted().toArray(),
                        partitionRecordFetchResult.getRecords().stream().mapToLong(ConsumerRecord::offset).toArray()));
    }

    @Test
    public void doReverseFetchByOffsetsLessLimitTwoPartitionsTest() {

        ConsumerMock consumerMock = ConsumerMock.builder()
                .topicName(TOPIC_NAME)
                .offsetRanges(new OffsetRange[]{
                        new OffsetRange(100L, true, 1045L, true),
                        new OffsetRange(100L, true, 998L, true) })
                .realPollBeginningPosition(new long[]{110L, 100L})
                .startingOffsets(new long[]{100L, 130L})
                .build();

        consumerMock.before();

        doReturn(ConfluentUtils.generateOffsetRanges(consumerMock.getTopicName(),
                consumerMock.getStartingOffsets(),consumerMock.getOffsetRanges()))
                .when(biDirectionalTopicRecordFetcherSpy).fetchOffsetRanges(anyCollection());

        RecordFetchResult<String, String> results = biDirectionalTopicRecordFetcherSpy
                .doReverseFetchByOffsets(consumerMock.getConsumer(),
                        ConfluentUtils.generateStartingOffsets(consumerMock.getTopicName(),consumerMock.getStartingOffsets()),
                        20, null, TIMEOUT_MS);

        Assertions.assertEquals(10, results.getRecords().size());
        results.getResults().forEach((topicPartition, partitionRecordFetchResult) -> {
                if(topicPartition.partition()==0) {
                    Assertions.assertTrue(partitionRecordFetchResult.getRecords().isEmpty());
                } else {
                    Assertions.assertArrayEquals(
                            new long[] {121, 122, 123, 124, 125, 126, 127, 128, 129, 130},
                            partitionRecordFetchResult.getRecords().stream().mapToLong(
                                    ConsumerRecord::offset).toArray());
                }
        });
    }

    @Test
    public void doReverseFetchByOffsetsFilterTest() {

        ConsumerMock consumerMock = ConsumerMock.builder()
                                                .topicName(TOPIC_NAME)
                                                .offsetRanges(new OffsetRange[]{
                                                        new OffsetRange(100L, true, 200L, true)})
                                                .realPollBeginningPosition(new long[]{110L})
                                                .startingOffsets(new long[]{200L})
                                                .build();

        consumerMock.before();

        doReturn(ConfluentUtils.generateOffsetRanges(consumerMock.getTopicName(),
                                                     consumerMock.getStartingOffsets(),
                                                     consumerMock.getOffsetRanges()))
                .when(biDirectionalTopicRecordFetcherSpy).fetchOffsetRanges(anyCollection());

        RecordFetchResult<String, String> results = biDirectionalTopicRecordFetcherSpy
                .doReverseFetchByOffsets(consumerMock.getConsumer(),
                                         ConfluentUtils.generateStartingOffsets(consumerMock.getTopicName(),
                                                                                consumerMock.getStartingOffsets()),
                                         100, consumerRecord -> consumerRecord.offset()<125, TIMEOUT_MS);

        Assertions.assertEquals(15, results.getRecords().size());
        results.getResults().forEach((topicPartition, partitionRecordFetchResult) -> {
        Assertions.assertArrayEquals(LongStream.range(110, 125).toArray(),
                                 partitionRecordFetchResult.getRecords().stream().mapToLong(
                                    ConsumerRecord::offset).toArray());

        });
    }

    @Test
    public void doReverseFetchByOffsetsFilterInAMiddleTest() {

        ConsumerMock consumerMock = ConsumerMock.builder()
                                                .topicName(TOPIC_NAME)
                                                .offsetRanges(new OffsetRange[]{
                                                        new OffsetRange(100L, true, 200L, true)})
                                                .realPollBeginningPosition(new long[]{110L})
                                                .startingOffsets(new long[]{200L})
                                                .build();

        consumerMock.before();

        doReturn(ConfluentUtils.generateOffsetRanges(consumerMock.getTopicName(),
                                                     consumerMock.getStartingOffsets(),
                                                     consumerMock.getOffsetRanges()))
                .when(biDirectionalTopicRecordFetcherSpy).fetchOffsetRanges(anyCollection());

        RecordFetchResult<String, String> results = biDirectionalTopicRecordFetcherSpy
                .doReverseFetchByOffsets(consumerMock.getConsumer(),
                                         ConfluentUtils.generateStartingOffsets(consumerMock.getTopicName(),
                                                                                consumerMock.getStartingOffsets()),
                                         100, consumerRecord -> consumerRecord.offset()<125 && consumerRecord.offset()>=115, TIMEOUT_MS);

        Assertions.assertEquals(10, results.getRecords().size());
        results.getResults().forEach((topicPartition, partitionRecordFetchResult) -> {
            Assertions.assertArrayEquals(LongStream.range(115, 125).toArray(),
                                     partitionRecordFetchResult.getRecords().stream().mapToLong(
                                             ConsumerRecord::offset).toArray());

        });
    }
}