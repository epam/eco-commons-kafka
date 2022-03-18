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
package com.epam.eco.commons.kafka.consumer.advanced;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.epam.eco.commons.kafka.KafkaUtils;
import com.epam.eco.commons.kafka.TopicPartitionComparator;
import com.epam.eco.commons.kafka.config.ConsumerConfigBuilder;
import com.epam.eco.commons.kafka.config.ProducerConfigBuilder;

/**
 * @author Andrei_Tytsik
 */
public class AdvancedConsumerIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(AdvancedConsumerIT.class);

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    private static final String TOPIC_PREFIX = "adv_cons_test_32_";
    private static final boolean GENERATE_TOPIC_NAMES = false;
    private static final boolean PUBLISH_RECORDS = false;
    private static final int NUMBER_OF_TOPICS = 30;
    private static final int NUMBER_OF_MESSAGES_PER_TOPIC = 100000;
    private static final String GROUP_ID_PREFIX = "adv_cons_test_grp_32_";
    private static final int NUMBER_OF_CONSUMERS = 10;
    private static final long COMPLETION_TIMEOUT_MS = 120000;

    private static final Map<String, Object> PRODUCER_CONFIG = ProducerConfigBuilder.
            withEmpty().
            bootstrapServers(BOOTSTRAP_SERVERS).
            acksAll().
            retriesMax().
            maxBlockMsMax().
            requestTimeoutMs(60000).
            lingerMs(20).
            keySerializerString().
            valueSerializerString().
            build();

    private static final Map<String, Object> CONSUMER_CONFIG = ConsumerConfigBuilder.
            withEmpty().
            bootstrapServers(BOOTSTRAP_SERVERS).
            autoOffsetResetEarliest().
            maxPollRecordsMax().
            keyDeserializerString().
            valueDeserializerString().
            build();

    public static void main(String[] args) throws Exception {
        List<String> topics = getTopicNames();

        LOGGER.info("Publishing data to topics");

        publishMessagesToTopics(topics);

        String groupId = getGroupId();
        KeysCollectingHandler handler = new KeysCollectingHandler();

        List<AdvancedConsumer<String, String>> consumers =
                initAndStartConsumers(groupId, topics, handler, 1000);

        LOGGER.info("All consumers started");

        Thread.sleep(5000);

        LOGGER.info("Changing subscription randomly for random consumers");

        changeSubscriptionRandomlyForRandomConsumers(consumers, topics, 0.3);

        Thread.sleep(5000);

        LOGGER.info("Shutting down random consumers");

        shutdownRandomConsumers(consumers, 0.4);

        Thread.sleep(2000);

        LOGGER.info("Restoring subscription for all consumers");

        changeSubscriptionForAllConsumers(consumers, topics);

        Thread.sleep(COMPLETION_TIMEOUT_MS);

        LOGGER.info("Shutting down all consumers");

        shutdownAllConsumers(consumers);

        handler.printKeysInfo();
        handler.printOffsetChains();
    }

    private static List<String> getTopicNames() {
        List<String> topics = new ArrayList<>();
        for (int i = 0; i < NUMBER_OF_TOPICS; i++) {
            topics.add(TOPIC_PREFIX + (GENERATE_TOPIC_NAMES ? UUID.randomUUID() : i));
        }
        return topics;
    }

    private static String getGroupId() {
        return GROUP_ID_PREFIX + UUID.randomUUID();
    }

    private static void publishMessagesToTopics(List<String> topics) {
        if (!PUBLISH_RECORDS) {
            return;
        }

        AtomicInteger ackCount = new AtomicInteger(0);
        Callback ackCallback = new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null) {
                    ackCount.incrementAndGet();
                }
            }
        };

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(PRODUCER_CONFIG)) {
            AtomicInteger count = new AtomicInteger(0);
            topics.forEach(topic -> {
                for (int i = 0; i < NUMBER_OF_MESSAGES_PER_TOPIC; i++) {
                    producer.send(new ProducerRecord<>(topic, "" + count.getAndIncrement(), null), ackCallback);
                }
                producer.flush();
                LOGGER.info("Topic {} done", topic);
            });
            LOGGER.info("{} records published, {} records acknowledged", count.get(), ackCount.get());
            if (count.get() != ackCount.get()) {
                throw new RuntimeException("Number of published records != number of acknowledged records");
            }
        }
    }

    private static List<AdvancedConsumer<String, String>> initAndStartConsumers(
            String groupId,
            List<String> topics,
            Consumer<RecordBatchIterator<String, String>> handler,
            long delay) throws InterruptedException {
        List<AdvancedConsumer<String, String>> consumers = new ArrayList<>();
        for (int i = 0; i < NUMBER_OF_CONSUMERS; i++) {
            AdvancedConsumer<String, String> consumer = new AdvancedConsumer<>(
                    topics,
                    ConsumerConfigBuilder.with(CONSUMER_CONFIG).groupId(groupId).build(),
                    handler);
            consumer.start();
            consumers.add(consumer);
            LOGGER.info("Consumer #{} started", i);

            Thread.sleep(new Random().nextInt((int)delay));
        }
        return consumers;
    }

    private static void changeSubscriptionForAllConsumers(
            List<AdvancedConsumer<String, String>> consumers,
            List<String> topics) {
        for (AdvancedConsumer<String, String> consumer : consumers) {
            LOGGER.info("Changing subsription for consumer #{}: {}", consumers.indexOf(consumer), topics);
            consumer.subscribe(topics);
        }
    }

    private static void changeSubscriptionRandomlyForRandomConsumers(
            List<AdvancedConsumer<String, String>> consumers,
            List<String> topics,
            double percentage) {
        for (Integer consumerIdx : getRandomIndexes(consumers.size(), percentage)) {
            AdvancedConsumer<String, String> consumer = consumers.get(consumerIdx);
            List<String> topicsNew = topics.subList(0, new Random().nextInt(topics.size()));
            LOGGER.info("Changing subsription for consumer #{}: {}", consumerIdx, topicsNew);
            consumer.subscribe(topicsNew);
        }
    }

    private static void shutdownAllConsumers(List<AdvancedConsumer<String, String>> consumers) throws InterruptedException {
        for (AdvancedConsumer<String, String> consumer : consumers) {
            LOGGER.info("Shutting down consumer #{}", consumers.indexOf(consumer));
            consumer.shutdown();
        }
        consumers.clear();
    }

    private static void shutdownRandomConsumers(
            List<AdvancedConsumer<String, String>> consumers,
            double percentage) throws InterruptedException {
        for (Integer consumerIdx : getRandomIndexes(consumers.size(), percentage)) {
            AdvancedConsumer<String, String> consumer = consumers.get(consumerIdx.intValue());
            LOGGER.info("Shutting down consumer #{}", consumerIdx);
            consumer.shutdown();
        }
    }

    private static Collection<Integer> getRandomIndexes(int size, double percentage) {
        Set<Integer> randomConsumerIndexes = new HashSet<>();
        while (randomConsumerIndexes.size() < size * percentage) {
            randomConsumerIndexes.add(new Random().nextInt(size));
        }
        return randomConsumerIndexes;
    }

    private static class KeysCollectingHandler implements Consumer<RecordBatchIterator<String, String>> {

        private final List<String> keys = Collections.synchronizedList(new ArrayList<>());
        private final Map<TopicPartition, List<Object>> offsetChains =
                Collections.synchronizedMap(new TreeMap<>(TopicPartitionComparator.INSTANCE));

        @Override
        public void accept(RecordBatchIterator<String, String> iterator) {
            for (ConsumerRecord<String, String> record : iterator) {
                keys.add(record.key());
                iterator.updateCommitPosition();
            }

            saveOffsetChain(iterator);

            try {
                Thread.sleep(Math.max(1000, new Random().nextInt(3000)));
            } catch (InterruptedException ie) {
            }

            LOGGER.info("{} keys collected", keys.size());
        }

        private void saveOffsetChain(RecordBatchIterator<String, String> iterator) {
            Map<TopicPartition, Long> smallestOffsets = KafkaUtils.extractSmallestOffsets(
                    ((InternalRecordBatchIterator<String, String>)iterator).getRecords());

            Map<TopicPartition, Long> offsetsToCommit = iterator.buildOffsetsToCommit();
            offsetsToCommit.forEach((partition, offset) -> {
                List<Object> chain = offsetChains.get(partition);
                if (chain == null) {
                    chain = new ArrayList<>();
                    offsetChains.put(partition, chain);
                }
                chain.add(
                        new Object[]{
                                smallestOffsets.get(partition),
                                offsetsToCommit.get(partition)});
            });
        }

        private void printKeysInfo() {
            Set<String> keysUnique = new HashSet<>();
            keysUnique.addAll(keys);

            LOGGER.info(
                    "Keys published = {}, collected = {}, unique = {}",
                    NUMBER_OF_TOPICS * NUMBER_OF_MESSAGES_PER_TOPIC, keys.size(), keysUnique.size());
        }

        private void printOffsetChains() {
            offsetChains.forEach((partition, chain) -> {
                StringBuilder builder = new StringBuilder();
                long lastTo = -1;
                for (Object pair : chain) {
                    long from = (long)((Object[])pair)[0];
                    long to = (long)((Object[])pair)[1];

                    if (builder.length() > 0) {
                        builder.append("-");
                    }

                    if (lastTo != -1 && lastTo != from) {
                        builder.append("BROKEN-");
                    }

                    lastTo = to;

                    builder.append(String.format("[%s..%s]", from, to));
                }
                LOGGER.info("Offset chain for {}: {}", partition, builder);
            });
        }

    }

}
