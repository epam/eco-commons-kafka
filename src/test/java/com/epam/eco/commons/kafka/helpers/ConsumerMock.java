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

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import com.epam.eco.commons.kafka.OffsetRange;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/**
 * @author Mikhail_Vershkov
 */

public class ConsumerMock<K,V> {

    private final static int MAX_POLLED_MESSAGES = 2000;
    private String topicName;
    private long[] startingOffsets;
    private long[] currentPollingPosition;
    private long[] realPollBeginningPosition;
    private final KafkaConsumer<K, V> consumer = mock(KafkaConsumer.class);
    private OffsetRange[] offsetRanges;

    public void before() {

        doAnswer(invocation -> {
            TopicPartition partition = invocation.getArgument(0);
            return currentPollingPosition[partition.partition()];
        }).when(consumer).position(any(TopicPartition.class));

        doAnswer(invocation -> new HashSet<>(ConfluentUtils.generateTopicPartitions(topicName,startingOffsets)))
                .when(consumer).assignment();

        doAnswer(invocation -> {
            TopicPartition partition = invocation.getArgument(0);
            Long offset = invocation.getArgument(1);
            currentPollingPosition[partition.partition()] = offset;
            return null;
        }).when(consumer).seek(any(TopicPartition.class), anyLong());

        doAnswer(invocation -> fakePoll()).when(consumer).poll(any(Duration.class));

    }

    public KafkaConsumer<K,V> getConsumer() {
        return consumer;
    }

    public List<ConsumerRecord<String,String>> generateNextConsumerRecord(TopicPartition topicPartition) {
        long currentPosition = currentPollingPosition[topicPartition.partition()];

        if(currentPosition>offsetRanges[topicPartition.partition()].getLargest()) {
            return Collections.emptyList();
        }
        if(currentPosition<realPollBeginningPosition[topicPartition.partition()]) {
            currentPollingPosition[topicPartition.partition()] = realPollBeginningPosition[topicPartition.partition()];
            currentPosition = realPollBeginningPosition[topicPartition.partition()];
        } else {
            currentPollingPosition[topicPartition.partition()] = currentPosition + 1L;
        }
        return List.of( new ConsumerRecord<>(topicName,topicPartition.partition(),
                currentPosition,"key-"+currentPosition,"value-"+currentPosition));
    }

    public ConsumerRecords<String, String> fakePoll() {
        Map<TopicPartition, List<ConsumerRecord<String,String>>> configMap =
                ConfluentUtils.generateTopicPartitions(topicName,startingOffsets).stream()
                        .collect(Collectors.toMap(
                                topicPartition -> topicPartition,
                                topicPartition -> IntStream.range(0,MAX_POLLED_MESSAGES)
                                        .mapToObj(ii->generateNextConsumerRecord(topicPartition))
                                        .flatMap(Collection::stream)
                                        .collect(Collectors.toList())

                        ));
        return new ConsumerRecords<>(configMap);
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public void setStartingOffsets(long[] startingOffsets) {
        this.startingOffsets = startingOffsets;
    }

    public void setRealPollBeginningPosition(long[] realPollBeginningPosition) {
        this.currentPollingPosition = Arrays.copyOf(realPollBeginningPosition, realPollBeginningPosition.length);
        this.realPollBeginningPosition = realPollBeginningPosition;
    }

    public void setOffsetRanges(OffsetRange[] offsetRanges) {
        this.offsetRanges = offsetRanges;
    }

    public String getTopicName() {
        return topicName;
    }

    public long[] getStartingOffsets() {
        return startingOffsets;
    }

    public long[] getCurrentPollingPosition() {
        return currentPollingPosition;
    }

    public long[] getRealPollBeginningPosition() {
        return realPollBeginningPosition;
    }

    public OffsetRange[] getOffsetRanges() {
        return offsetRanges;
    }

    public static Builder builder() {
        return new Builder();
    }

    static class Builder {

        private final ConsumerMock consumerMock = new ConsumerMock();

        Builder topicName(String topicName) {
            consumerMock.topicName = topicName;
           return this;
        }
        Builder startingOffsets(long[] startingOffsets) {
            consumerMock.startingOffsets = startingOffsets;
            return this;
        }
        Builder realPollBeginningPosition(long[] realPollBeginningPosition) {
            consumerMock.realPollBeginningPosition = realPollBeginningPosition;
            return this;
        }
        Builder offsetRanges(OffsetRange[] offsetRanges) {
            consumerMock.offsetRanges = offsetRanges;
            return this;
        }

        public ConsumerMock build() {
            consumerMock.currentPollingPosition =
                    Arrays.copyOf(consumerMock.realPollBeginningPosition, consumerMock.realPollBeginningPosition.length);
            return consumerMock;
        }

    }


}
