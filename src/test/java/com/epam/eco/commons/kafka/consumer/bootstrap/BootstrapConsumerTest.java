package com.epam.eco.commons.kafka.consumer.bootstrap;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections4.ListUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.epam.eco.commons.kafka.config.AdminClientConfigBuilder;
import com.epam.eco.commons.kafka.config.ConsumerConfigBuilder;
import com.epam.eco.commons.kafka.config.ProducerConfigBuilder;
import com.epam.eco.commons.kafka.helpers.KafkaTestHelper;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BootstrapConsumerTest {

    private static final String BOOTSTRAP_SERVERS = "kafka.sandbox.datahub.epam.com:9092";
    private static final String TOPIC_NAME = "bootstrap-consumer-test-topic";
    private static final Logger log = LoggerFactory.getLogger(BootstrapConsumerTest.class);

    private final KafkaTestHelper helper = new KafkaTestHelper(
            AdminClient.create(
                    AdminClientConfigBuilder.withEmpty()
                            .bootstrapServers(BOOTSTRAP_SERVERS)
                            .build()
            )
    );

    @BeforeEach
    void init() {
        helper.initTopic(TOPIC_NAME);
    }

    @AfterEach
    void cleanup() {
        helper.deleteTopic(TOPIC_NAME);
    }

    @Test
    @Disabled
    void readsAllConsumerOffsets() {
        BootstrapConsumer<byte[], byte[], List<ConsumerRecord<byte[], byte[]>>> consumer =
                BootstrapConsumer.<byte[], byte[], List<ConsumerRecord<byte[], byte[]>>>builder()
                        .topicName("__consumer_offsets")
                        .consumerConfig(
                                ConsumerConfigBuilder.withEmpty()
                                        .bootstrapServers(BOOTSTRAP_SERVERS)
                                        .keyDeserializerByteArray()
                                        .valueDeserializerByteArray().build()
                        )
                        .offsetInitializer(TimestampOffsetInitializer.oneDayBefore())
                        .bootstrapTimeoutInMs(5 * 60 * 1000)
                        .recordCollector(new ToListRecordCollector<>())
                        .build();

        List<ConsumerRecord<byte[], byte[]>> result = consumer.fetch();
        Assertions.assertFalse(result.isEmpty());

        consumer.close();
    }

    @Test
    void consumesForceCompactedTopic() throws Exception {
        int numberOfRuns = 5;
        for (int i = 0; i < numberOfRuns; i++) {
            doConsumeForceCompactedTopic();
        }
    }

    private void doConsumeForceCompactedTopic() throws Exception {
        helper.initTopic(TOPIC_NAME, 1);
        helper.setTopicForceCompact(TOPIC_NAME);

        try (Producer<String, String> producer = buildProducer()) {
            producer.send(record("key-1", "value-1-1"));
            producer.send(record("key-2", "value-2-1"));
            producer.send(record("key-2", "value-2-2"));
            producer.flush();

            producer.send(record("key-1", "value-1-2"));
            producer.flush();
        }

        Thread.sleep(3000L);

        try (BootstrapConsumer<String, String, List<ConsumerRecord<String, String>>> consumer = buildConsumer()) {
            Map<String, String> valueMap = latestValues(consumer.fetch());
            assertEquals("value-1-2", valueMap.get("key-1"));
            assertEquals("value-2-2", valueMap.get("key-2"));
        }
    }

    @Test
    void consumesDeleteTopic() {
        try (Producer<String, String> producer = buildProducer()) {
            producer.send(record("key-1", "value-1-1"));
            producer.send(record("key-2", "value-2-1"));
            producer.send(record("key-2", "value-2-2"));
            producer.send(record("key-1", "value-1-2"));
            producer.flush();

            try (BootstrapConsumer<String, String, List<ConsumerRecord<String, String>>> consumer = buildConsumer()) {
                List<ConsumerRecord<String, String>> initialRecords = consumer.fetch();
                assertEquals(4, initialRecords.size());
                Map<String, List<String>> initialValues = allValues(initialRecords);

                assertEquals(List.of("value-1-1", "value-1-2"), initialValues.get("key-1"));
                assertEquals(List.of("value-2-1", "value-2-2"), initialValues.get("key-2"));

                producer.send(record("key-1", "value-1-3"));
                producer.send(record("key-3", "value-3-1"));
                producer.flush();

                List<ConsumerRecord<String, String>> additionalRecords = consumerFetch(consumer);

                assertEquals(2, additionalRecords.size());
                Map<String, List<String>> additionalValues = allValues(additionalRecords);

                assertEquals(List.of("value-1-3"), additionalValues.get("key-1"));
                assertEquals(List.of("value-3-1"), additionalValues.get("key-3"));
            }
        }
    }

    private List<ConsumerRecord<String, String>> consumerFetch(BootstrapConsumer<String, String,
            List<ConsumerRecord<String, String>>> consumer) {
        List<ConsumerRecord<String, String>> result = new ArrayList<>();
        while (result.addAll(consumer.fetch())) {
            log.info("fetch result {}", result.size());
        }
            return result;
    }

    @Test
    void consumesCompactedTopic() {
        helper.setTopicCompact(TOPIC_NAME);

        try (Producer<String, String> producer = buildProducer()) {
            producer.send(record("key-1", "value-1-1"));
            producer.send(record("key-2", "value-2-1"));
            producer.send(record("key-2", "value-2-2"));
            producer.send(record("key-1", "value-1-2"));
            producer.flush();

            try (BootstrapConsumer<String, String, List<ConsumerRecord<String, String>>> consumer = buildConsumer()) {
                Map<String, String> initialValues = latestValues(consumer.fetch());
                assertEquals("value-1-2", initialValues.get("key-1"));
                assertEquals("value-2-2", initialValues.get("key-2"));

                producer.send(record("key-1", "value-1-3"));
                producer.send(record("key-3", "value-3-1"));
                producer.flush();

                List<ConsumerRecord<String, String>> additionalRecords = consumerFetch(consumer);
                assertEquals(2, additionalRecords.size());

                Map<String, String> additionalValues = latestValues(additionalRecords);
                assertEquals("value-1-3", additionalValues.get("key-1"));
                assertEquals("value-3-1", additionalValues.get("key-3"));
            }
        }
    }

    private Producer<String, String> buildProducer() {
        return new KafkaProducer<>(
                ProducerConfigBuilder.withEmpty()
                        .bootstrapServers(BOOTSTRAP_SERVERS)
                        .keySerializerString()
                        .valueSerializerString()
                        .acksAll()
                        .build()
        );
    }

    private BootstrapConsumer<String, String, List<ConsumerRecord<String, String>>> buildConsumer() {
        return BootstrapConsumer.<String, String, List<ConsumerRecord<String, String>>>builder()
                .topicName(TOPIC_NAME)
                .consumerConfig(
                        ConsumerConfigBuilder.withEmpty()
                                .bootstrapServers(BOOTSTRAP_SERVERS)
                                .keyDeserializerString()
                                .valueDeserializerString()
                                .fetchMaxBytes(5500000)
                                .maxPollRecords(100000)
                                .build()
                )
                .fetchPollTimeout(Duration.ofSeconds(5))
                .bootstrapTimeoutInMs(3600_000L)
                .recordCollector(new ToListRecordCollector<>())
                .build();
    }

    private ProducerRecord<String, String> record(String key, String value) {
        return new ProducerRecord<>(TOPIC_NAME, key, value);
    }

    private Map<String, String> latestValues(List<ConsumerRecord<String, String>> records) {
        return records.stream()
                .collect(Collectors.toMap(
                        ConsumerRecord::key,
                        ConsumerRecord::value,
                        (existing, replacement) -> replacement
                ));
    }

    private Map<String, List<String>> allValues(List<ConsumerRecord<String, String>> records) {
        return records.stream()
                .collect(Collectors.toMap(
                        ConsumerRecord::key,
                        record -> Collections.singletonList(record.value()),
                        ListUtils::union
                ));
    }
}