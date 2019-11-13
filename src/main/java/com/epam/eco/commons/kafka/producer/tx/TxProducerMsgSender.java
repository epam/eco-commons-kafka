/*
 * Copyright 2019 EPAM Systems
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.  You may obtain a copy
 * of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.epam.eco.commons.kafka.producer.tx;

import java.time.Duration;
import java.util.List;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.singletonMap;

/**
 * @author Aliaksei_Valyaev
 */
final class TxProducerMsgSender<K, V> {

    private static final Logger log = LoggerFactory.getLogger(TxProducerMsgSender.class);

    private final TopicPartition inTopic;
    private final String consumerGroup;
    private final Producer<K, V> producer;
    private final SendFailCallback<K, V> callback;

    private volatile boolean closed;

    TxProducerMsgSender(
            TopicPartition inTopic,
            String consumerGroup,
            Producer<K, V> producer,
            SendFailListener listener
    ) {
        this.inTopic = inTopic;
        this.consumerGroup = consumerGroup;
        this.producer = producer;
        this.callback = new SendFailCallback<>(producer, listener);

        this.producer.initTransactions();
    }

    /**
     * asynchronously sends messages to kafka within single transaction.
     * commits biggest offset of messages within the same transaction
     *
     * @param msgs - messages to send
     * @return - true if messages were sent successfully, false otherwise
     */
    boolean send(List<KafkaTxMsg<K, V>> msgs) {
        try {
            sendInterruptibly(msgs);
            return true;
        } catch (InterruptException e) {
            Thread.currentThread().interrupt();
            log.warn("send interrupted {}", e.getMessage());
            close();
            return false;
        } catch (Exception e) {
            log.warn("error while sending msg {}", e.getMessage());
            producer.abortTransaction();
            return false;
        }
    }

    private void sendInterruptibly(List<KafkaTxMsg<K, V>> msgs) {
        if (msgs.isEmpty()) {
            return;
        }

        log.info("sending batch of size: {}, consumer-group: {}, in-topic: {}",
                msgs.size(), consumerGroup, inTopic);

        producer.beginTransaction();
        log.debug("tx begin. batch of size: {}, consumer-group: {}, in-topic: {}",
                msgs.size(), consumerGroup, inTopic);

        long offSetToCommit = -1;

        for (KafkaTxMsg<K, V> msg : msgs) {
            if (msg.getOffset() > offSetToCommit) {
                offSetToCommit = msg.getOffset();
            }
            if (Thread.interrupted()) {
                Thread.currentThread().interrupt();
                close();
                return;
            }
            producer.send(
                    new ProducerRecord<>(msg.getOutTopic(), msg.getKey(), msg.getValue()),
                    callback
            );
        }
        log.debug("msgs sent. batch of size: {}, consumer-group: {}, in-topic: {}",
                msgs.size(), consumerGroup, inTopic);

        offSetToCommit = offSetToCommit + 1;

        producer.sendOffsetsToTransaction(
                singletonMap(inTopic, new OffsetAndMetadata(offSetToCommit)),
                consumerGroup
        );
        log.debug("offset sent. batch of size: {}, consumer-group: {}, in-topic: {}",
                msgs.size(), consumerGroup, inTopic);

        producer.commitTransaction();

        log.info("committed batch of size: {}, consumer-group: {}, in-topic: {}, committed offset: {}",
                msgs.size(), consumerGroup, inTopic, offSetToCommit);
    }

    synchronized void close() {
        if (closed) {
            return;
        }

        doQuietly(producer::abortTransaction);
        doQuietly(() -> producer.close(Duration.ofMinutes(0)));

        closed = true;
    }

    private static void doQuietly(Runnable runnable) {
        try {
            runnable.run();
        } catch (Exception e) {
            log.warn("msg sender error {}", e.getMessage());
        }
    }

    private static class SendFailCallback<K, V> implements Callback {

        private final Producer<K, V> producer;
        private final SendFailListener listener;

        SendFailCallback(Producer<K, V> producer, SendFailListener listener) {
            this.producer = producer;
            this.listener = listener;
        }

        @Override
        public void onCompletion(RecordMetadata metadata, Exception e) {
            if (e != null) {
                log.error("error in msg send callback", e);
                doQuietly(producer::abortTransaction);
                listener.onFail(e);
            }
        }
    }

}
