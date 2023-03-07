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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import com.epam.eco.commons.kafka.OffsetRange;

import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

/**
 * @author Mikhail_Vershkov
 */

@RunWith(MockitoJUnitRunner.class)
public class TopicRecordFetcherTest {
    private final static long[] STARTING_OFFSETS = {13, 12, 5, 11, 15};
    private final static long[] REAL_POLL_BEGINNING_POSITION = {13, 12, 5, 11, 15};
    private final static OffsetRange[] OFFSET_RANGES = {
            new OffsetRange(5, true, 15, true),
            new OffsetRange(4, true, 15, true),
            new OffsetRange(4, true, 12, true),
            new OffsetRange(5, true, 17, true),
            new OffsetRange(7, true, 17, true)};

    private final static long[][] EXPECTED_OFFSETS = {
            {13, 14, 15},
            {12, 13, 14, 15},
            {5, 6, 7, 8, 9},
            {11, 12, 13, 14, 15},
            {15, 16, 17}};
    private final static long[][] EXPECTED_OFFSETS_LESS = {
            {13, 14, 15},
            {12, 13, 14, 15},
            {5, 6, 7, 8, 9},
            {11, 12, 13, 14, 15},
            {15, 16, 17}};
    private final static int EXPECTED_RESULT_SIZE = 20;

    private final static String TOPIC_NAME = "test-topic";
    private final static int LIMIT = 25;
    private final static long TIMEOUT_MS = 1000000L;

    private final TopicRecordFetcher<String, String> topicRecordFetcher = TopicRecordFetcher.with(new HashMap<>());

    private final TopicRecordFetcher<String, String> topicRecordFetcherSpy = spy(topicRecordFetcher);

    private final Map<TopicPartition, Long> offsets = ConfluentUtils.generateStartingOffsets(TOPIC_NAME,STARTING_OFFSETS);

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
                .when(topicRecordFetcherSpy).fetchOffsetRanges(anyCollection());


        RecordFetchResult<String, String> results = topicRecordFetcherSpy
                .doFetchByOffsets(consumerMock.getConsumer(), offsets, LIMIT, null, TIMEOUT_MS);
        Assert.assertEquals(EXPECTED_RESULT_SIZE, results.getRecords().size());
        results.getResults().forEach((topicPartition, partitionRecordFetchResult) ->
                Assert.assertArrayEquals(Arrays.stream(EXPECTED_OFFSETS[topicPartition.partition()]).sorted().toArray(),
                        partitionRecordFetchResult.getRecords().stream().mapToLong(ConsumerRecord::offset).toArray()));
    }

}