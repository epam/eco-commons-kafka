/*******************************************************************************
 *  Copyright 2024 EPAM Systems
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
package com.epam.eco.commons.kafka.consumer.advanced;

import com.epam.eco.commons.kafka.config.ConsumerConfigBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

class AdvancedConsumerTest {

    private static final String TOPIC_NAME = "test";
    private static final int PARTITION = 0;

    @SuppressWarnings("unchecked")
    @Test
    void testRetryOnAuthException() throws InterruptedException {
        var authExceptionRetryIntervalMs = 100;
        var consumerConfig = ConsumerConfigBuilder.withEmpty()
                .minRequiredConfigs()
                .authExceptionRetryIntervalMs(authExceptionRetryIntervalMs)
                .build();

        var mockConsumer = mock(KafkaConsumer.class);
        var expectedRecord = expectedRecord();

        var latch = setupMockConsumer(mockConsumer);

        var consumedRecords = new ConcurrentLinkedQueue<ConsumerRecord<String, String>>();

        var advancedConsumer = new AdvancedConsumer<String, String>(
                List.of(TOPIC_NAME),
                consumerConfig,
                recordIterator -> recordIterator.forEach(consumedRecords::offer),
                (config) -> mockConsumer
        );

        advancedConsumer.start();

        Thread.sleep(500);

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        advancedConsumer.shutdown(5, TimeUnit.SECONDS);

        assertEquals(1, consumedRecords.size());
        var actualRecord = consumedRecords.peek();
        assertAll(
                () -> assertEquals(expectedRecord.key(), actualRecord.key()),
                () -> assertEquals(expectedRecord.value(), actualRecord.value())
        );
    }

    @SuppressWarnings("unchecked")
    @Test
    void testFailOnAuthException() throws InterruptedException {
        var consumerConfig = ConsumerConfigBuilder.withEmpty()
                .minRequiredConfigs()
                .build();

        var mockConsumer = mock(KafkaConsumer.class);
        var latch = setupMockConsumer(mockConsumer);

        var consumedRecords = new ConcurrentLinkedQueue<ConsumerRecord<String, String>>();

        var advancedConsumer = new AdvancedConsumer<String, String>(
                List.of(TOPIC_NAME),
                consumerConfig,
                recordIterator -> recordIterator.forEach(consumedRecords::offer),
                (config) -> mockConsumer
        );

        advancedConsumer.start();
        assertFalse(latch.await(5, TimeUnit.SECONDS));
        advancedConsumer.shutdown(5, TimeUnit.SECONDS);

        assertTrue(consumedRecords.isEmpty());
    }

    private CountDownLatch setupMockConsumer(KafkaConsumer<String, String> mockConsumer) {
        // Simulate calls to consumer.poll():
        //   - 1st and 2nd call throw AuthorizationException
        //   - 3d call returns non-empty ConsumerRecords
        //   - other calls return empty ConsumerRecords
        var latch = new CountDownLatch(3);

        doAnswer(__ -> {
            if (latch.getCount() > 1) {
                latch.countDown();
                throw new TopicAuthorizationException("test");
            } else if (latch.getCount() == 1) {
                latch.countDown();
                return new ConsumerRecords<>(Map.of(topicPartition(), List.of(expectedRecord())));
            }
            return new ConsumerRecords<>(emptyMap());
        }).when(mockConsumer).poll(any());

        doReturn(Set.of(TOPIC_NAME)).when(mockConsumer).subscription();

        return latch;
    }

    private static ConsumerRecord<String, String> expectedRecord() {
        return new ConsumerRecord<>(TOPIC_NAME, PARTITION, 0, "key", "value");
    }

    private static TopicPartition topicPartition() {
        return new TopicPartition(TOPIC_NAME, PARTITION);
    }
}