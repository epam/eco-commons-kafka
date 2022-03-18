/*******************************************************************************
 *  Copyright 2022 EPAM Systems
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
package com.epam.eco.commons.kafka.producer.tx;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.awaitility.Duration;
import org.awaitility.core.ThrowingRunnable;
import org.junit.After;
import org.junit.Test;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Aliaksei_Valyaev
 */
public class TxProducerFacadeTest {

    private static final String TEST_OUT_TOPIC = "test-out-topic";
    private static final String TEST_CONSUMER_GROUP = "test-consumer-group";
    private static final long DELAY = 100;

    private MockSendFailListener listener = new MockSendFailListener();
    private TxMockProducer<String, String> mock = new TxMockProducer<>();
    private TxProducerFacade<String, String> producer = new TxProducerFacade<>(
            TxProducerProps.builder()
                    .consumerGroup(TEST_CONSUMER_GROUP)
                    .maxMsgsSize(5)
                    .sendDelayMs(10)
                    .sendPeriodMs(50)
                    .threadPoolSize(1)
                    .txIdPrefix("tx-id-prefix")
                    .build(),
            (idPrefix, inTopic, configs) -> mock,
            listener
    );

    @After
    public void clear() throws Exception {
        mock.clear();
        listener.reset();
        producer.close();
    }

    @Test(expected = IllegalStateException.class)
    public void removeFailsForNotExistedTopic() {
        producer.removeProcessor(topic("not-existed", 0));
    }

    @Test
    public void removesTopic() {
        assertTrue(!mock.transactionInitialized());
        producer.initProcessor(topic("removed-topic", 1));
        assertTrue(mock.transactionInitialized());
        producer.removeProcessor(topic("removed-topic", 1));
        assertTrue(mock.closed());
    }

    @Test(expected = IllegalStateException.class)
    public void initFailsForExistedTopic() {
        producer.initProcessor(topic("existed", 0));
        assertTrue(mock.transactionInitialized());
        producer.initProcessor(topic("existed", 0));
    }

    @Test(expected = IllegalStateException.class)
    public void enqueueFailsForNotExistedTopic() {
        producer.enqueue(msg("not-existed", 0, 0, "key-1", "value-1"));
    }

    @Test
    public void producesByCnt() {
        producer.initProcessor(topic("t-in-topic", 2));

        producer.enqueue(msg("t-in-topic", 2, 0, "key-1", "value-1"));
        producer.enqueue(msg("t-in-topic", 2, 1, "key-2", "value-2"));
        producer.enqueue(msg("t-in-topic", 2, 2, "key-3", "value-3"));
        producer.enqueue(msg("t-in-topic", 2, 3, "key-4", "value-4"));
        producer.enqueue(msg("t-in-topic", 2, 4, "key-5", "value-5"));

        assertTrue(mock.transactionCommitted());
        assertEquals(asList(
                sentMsg("key-1", "value-1"),
                sentMsg("key-2", "value-2"),
                sentMsg("key-3", "value-3"),
                sentMsg("key-4", "value-4"),
                sentMsg("key-5", "value-5")
        ), mock.history());
        verifyCommittedOffset(singletonList(singletonMap(topic("t-in-topic", 2), 5L)));
    }

    @Test
    public void producesByTimeout() {
        producer.initProcessor(topic("t-in-topic", 3));

        producer.enqueue(msg("t-in-topic", 3, 0, "key-1", "value-1"));

        untilAsserted(() -> {
            assertTrue(mock.transactionCommitted());
            assertEquals(singletonList(sentMsg("key-1", "value-1")), mock.history());
            verifyCommittedOffset(singletonList(singletonMap(topic("t-in-topic", 3), 1L)));
        });

        producer.enqueue(msg("t-in-topic", 3, 1, "key-2", "value-2"));

        untilAsserted(() -> {
            assertTrue(mock.transactionCommitted());
            assertEquals(
                    asList(sentMsg("key-1", "value-1"), sentMsg("key-2", "value-2")),
                    mock.history()
            );
            verifyCommittedOffset(asList(
                    singletonMap(topic("t-in-topic", 3), 1L),
                    singletonMap(topic("t-in-topic", 3), 2L)
            ));
        });
    }

    @Test
    public void callsFailListener() {
        assertEquals(0, listener.getCounter());

        mock.doErrorOnSend();

        producer.initProcessor(topic("t-in-topic", 3));

        producer.enqueue(msg("t-in-topic", 3, 0, "key-1", "value-1"));
        producer.enqueue(msg("t-in-topic", 3, 1, "key-2", "value-2"));
        producer.enqueue(msg("t-in-topic", 3, 2, "key-3", "value-3"));
        producer.enqueue(msg("t-in-topic", 3, 3, "key-4", "value-4"));
        producer.enqueue(msg("t-in-topic", 3, 4, "key-5", "value-5"));

        producer.enqueue(msg("t-in-topic", 3, 5, "key-6", "value-6"));

        untilAsserted(() -> {
            assertTrue(listener.getCounter() >= 1);
            assertTrue(mock.transactionAborted());
            assertTrue(mock.history().isEmpty());
            assertTrue(mock.consumerGroupOffsetsHistory().isEmpty());
        });
    }

    @Test
    public void removeDoesNotInterruptTask() throws InterruptedException {
        mock.doDelays();

        producer.initProcessor(topic("t-in-topic-2", 10));

        producer.enqueue(msg("t-in-topic-2", 10, 0, "key-1", "value-1"));
        producer.enqueue(msg("t-in-topic-2", 10, 1, "key-2", "value-2"));

        Thread.sleep(DELAY);

        producer.removeProcessor(topic("t-in-topic-2", 10));

        untilAsserted(() -> {
            assertTrue(mock.transactionCommitted());
            assertEquals(
                    asList(
                            sentMsg("key-1", "value-1"),
                            sentMsg("key-2", "value-2")
                    ),
                    mock.history());
            verifyCommittedOffset(singletonList(singletonMap(topic("t-in-topic-2", 10), 2L)));
            assertTrue(mock.closed());
        });
    }

    @Test
    public void doesNotInterruptSynchronousSend() {
        mock.doDelays();

        producer.initProcessor(topic("t-in-topic-3", 9));

        new Thread(() -> {
            sleep();
            producer.removeProcessor(topic("t-in-topic-3", 9));
        }).start();

        producer.enqueue(msg("t-in-topic-3", 9, 2, "key-1", "value-1"));
        producer.enqueue(msg("t-in-topic-3", 9, 4, "key-2", "value-2"));
        producer.enqueue(msg("t-in-topic-3", 9, 7, "key-3", "value-3"));
        producer.enqueue(msg("t-in-topic-3", 9, 8, "key-4", "value-4"));
        producer.enqueue(msg("t-in-topic-3", 9, 33, "key-5", "value-5"));

        untilAsserted(() -> {
            assertTrue(mock.transactionCommitted());
            assertEquals(
                    asList(
                            sentMsg("key-1", "value-1"),
                            sentMsg("key-2", "value-2"),
                            sentMsg("key-3", "value-3"),
                            sentMsg("key-4", "value-4"),
                            sentMsg("key-5", "value-5")
                    ),
                    mock.history());
            verifyCommittedOffset(singletonList(singletonMap(topic("t-in-topic-3", 9), 34L)));
            assertTrue(mock.closed());
        });
    }

    @Test
    public void doesNotLoseMsgsOnError() {
        mock.doErrorOnCommit(true);

        producer.initProcessor(topic("t-in-topic-8", 8));

        producer.enqueue(msg("t-in-topic-8", 8, 1, "key-1", "value-1"));

        sleep();

        mock.doErrorOnCommit(false);
        mock.clearTxAborted();

        producer.enqueue(msg("t-in-topic-8", 8, 2, "key-2", "value-2"));

        untilAsserted(() -> {
            assertTrue(mock.transactionCommitted());
            assertEquals(
                    asList(sentMsg("key-1", "value-1"), sentMsg("key-2", "value-2")),
                    mock.history()
            );
            verifyCommittedOffset(singletonList(
                    singletonMap(topic("t-in-topic-8", 8), 3L)
            ));
        });
    }

    private KafkaTxMsg<String, String> msg(
            String inTopic,
            int partition,
            long offset,
            String key,
            String value
    ) {
        return KafkaTxMsg.<String, String>builder()
                .outTopic(TEST_OUT_TOPIC)
                .inTopic(topic(inTopic, partition))
                .offset(offset)
                .key(key)
                .value(value)
                .build();
    }

    private ProducerRecord<String, String> sentMsg(String key, String value) {
        return new ProducerRecord<>(TEST_OUT_TOPIC, key, value);
    }

    private TopicPartition topic(String topic, int partition) {
        return new TopicPartition(topic, partition);
    }

    private void verifyCommittedOffset(List<Map<TopicPartition, Long>> offsets) {
        List<Map<String, Map<TopicPartition, OffsetAndMetadata>>> expected = offsets.stream()
                .map(offset -> offset.entrySet().stream().findFirst())
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(e -> singletonMap(
                        TEST_CONSUMER_GROUP,
                        singletonMap(e.getKey(), new OffsetAndMetadata(e.getValue()))
                ))
                .collect(Collectors.toList());
        assertEquals(expected, mock.consumerGroupOffsetsHistory());
    }

    private void untilAsserted(ThrowingRunnable assertion) {
        await().atMost(Duration.TWO_SECONDS).untilAsserted(assertion);
    }

    private void sleep() {
        try {
            Thread.sleep(DELAY);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
