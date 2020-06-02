/*
 * Copyright 2020 EPAM Systems
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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.String.format;

/**
 * Implements atomic consume-process-produce approach using Kafka transactions while
 * preserving possibility to work under automatic partitions rebalance (every consuming
 * partition has separate kafka producer). Batches incoming messages (by timeout or by
 * messages count) in order to reduce number of network interactions (does single offset
 * commit per batch).
 *
 * @author Aliaksei_Valyaev
 */
public final class TxProducerFacade<K, V> implements ConsumerRebalanceListener, Closeable {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private final TxProducerProps props;
    private final ScheduledExecutorService executorService;
    private final TxProducerFactory<K, V> txProducerFactory;
    private final SendFailListener listener;
    private final Map<TopicPartition, TxProducerProcessor<K, V>> processors = new HashMap<>();

    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    public TxProducerFacade(
            TxProducerProps props,
            TxProducerFactory<K, V> txProducerFactory,
            SendFailListener listener
    ) {
        Validate.notNull(props, "null props");
        Validate.notNull(txProducerFactory, "null txProducerFactory");
        Validate.notNull(listener, "null listener");

        this.props = props;
        this.executorService = new ScheduledThreadPoolExecutor(props.getThreadPoolSize());
        ((ScheduledThreadPoolExecutor) this.executorService).setRemoveOnCancelPolicy(true);
        this.txProducerFactory = txProducerFactory;
        this.listener = listener;
    }

    public void enqueue(KafkaTxMsg<K, V> msg) {
        Validate.notNull(msg, "null msg");

        doWithinLock(readWriteLock.readLock(), () -> {
            if (!processors.containsKey(msg.getInTopic())) {
                // assuming guarantees regarding partition revoke/assign events
                throw new IllegalStateException(format(
                        "trying to add msg for not existed processor for partition %s",
                        msg.getInTopic()
                ));
            }
            processors.get(msg.getInTopic()).addMsg(msg);
        });
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        // remove all by topic since we can come out from zombie state
        partitions.forEach(this::removeProcessorsOfTopic);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        partitions.forEach(this::initProcessor);
    }

    @Override
    public void close() throws IOException {
        removeAllProcessors();

        executorService.shutdown();
    }

    // package-private for testing
    void initProcessor(TopicPartition partition) {
        doWithinLock(readWriteLock.writeLock(), () -> {
            if (processors.containsKey(partition)) {
                throw new IllegalStateException(format(
                        "trying to initialize already existed processor for partition %s",
                        partition
                ));
            }
            log.info("initializing tx producer for partition {}", partition);
            processors.put(
                    partition,
                    new TxProducerProcessor<>(
                            props,
                            new ScheduledTxProducerTask(executorService, props),
                            new TxProducerMsgSender<>(
                                    partition,
                                    props.getConsumerGroup(),
                                    txProducerFactory.create(
                                            props.getTxIdPrefix(),
                                            partition,
                                            props.getProducerCfg()
                                    ),
                                    listener
                            )
                    ).init()
            );
        });
    }

    // package-private for testing
    void removeProcessor(TopicPartition partition) {
        doWithinLock(readWriteLock.writeLock(), () -> {
            if (!processors.containsKey(partition)) {
                throw new IllegalStateException(format(
                        "trying to remove non existed processor for partition %s",
                        partition
                ));
            }
            log.info("removing tx producer for partition {}", partition);
            processors.remove(partition).close();
        });
    }

    // package-private for testing
    void removeProcessorsOfTopic(TopicPartition partition) {
        doWithinLock(
                readWriteLock.writeLock(),
                () -> new ArrayList<>(processors.keySet()).stream()
                        .filter(key -> key.topic().equals(partition.topic()))
                        .forEach(this::removeProcessor)
        );
    }

    // package-private for testing
    void removeAllProcessors() {
        doWithinLock(
                readWriteLock.writeLock(),
                () -> new ArrayList<>(processors.keySet()).forEach(this::removeProcessor)
        );
    }

    private void doWithinLock(Lock lock, Runnable runnable) {
        try {
            lock.lockInterruptibly();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("lock waiting interrupted {}", e.getMessage());
            return;
        }

        try {
            runnable.run();
        } finally {
            lock.unlock();
        }
    }

}
