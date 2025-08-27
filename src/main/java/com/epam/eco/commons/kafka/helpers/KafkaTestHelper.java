package com.epam.eco.commons.kafka.helpers;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.epam.eco.commons.kafka.AdminClientUtils;

import static java.lang.String.format;
import static java.util.Collections.singleton;

public final class KafkaTestHelper {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private static final Duration UNTIL_DURATION = Duration.ofSeconds(60);
    private static final Duration POLL_INTERVAL = Duration.ofMillis(2000);

    private static final int RETRIES_CNT = 5;

    private static final int TEST_TOPIC_PARTITIONS_NUMBER = 2;
    private static final short TEST_TOPIC_REPLICATION_FACTOR = 3;

    private final AdminClient adminClient;
    private final String testTopicNamePattern;
    private final String testConsumerGroupNamePattern;

    public KafkaTestHelper(AdminClient adminClient) {
        this(
                adminClient,
                "test-eco-commons=kafka-topic-name-template-%",
                "test-eco-commons=kafka-consumer-group-name-template-%"
        );
    }

    public KafkaTestHelper(
            AdminClient adminClient,
            String testTopicNamePattern,
            String testConsumerGroupNamePattern
    ) {
        Validate.notNull(adminClient, "null adminClient");
        Validate.notBlank(testTopicNamePattern, "blank testTopicNamePattern");
        Validate.notNull(testConsumerGroupNamePattern, "blank testConsumerGroupNamePattern");

        this.adminClient = adminClient;
        this.testTopicNamePattern = testTopicNamePattern;
        this.testConsumerGroupNamePattern = testConsumerGroupNamePattern;
    }

    public void initTopic(String topicName) {
        initTopic(topicName, TEST_TOPIC_PARTITIONS_NUMBER);
    }

    public void initTopic(String topicName, int numPartitions) {
        logger.debug("initTopic {}", topicName);
        deleteTopic(topicName);
        createTopic(topicName, numPartitions);
        logger.debug("initTopic {} finished", topicName);
    }

    public void setTopicCompact(String topicName) {
        logger.debug("setTopicCompact {}", topicName);
        AdminClientUtils.alterTopicConfigs(
                adminClient,
                topicName,
                Map.of(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT)
        );
        logger.debug("setTopicCompact {} finished", topicName);
    }

    public void setTopicForceCompact(String topicName) {
        logger.debug("setTopicForceCompact {}", topicName);
        AdminClientUtils.alterTopicConfigs(
                adminClient,
                topicName,
                Map.of(
                        TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT,
                        TopicConfig.DELETE_RETENTION_MS_CONFIG, String.valueOf(100L),
                        TopicConfig.SEGMENT_MS_CONFIG, String.valueOf(100L),
                        TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, String.valueOf(0.01),
                        TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG, String.valueOf(1000L),
                        TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, String.valueOf(500L)
                )
        );
        logger.debug("setTopicForceCompact {} finished", topicName);
    }

    public void deleteTopic(String topicName) {
        logger.debug("deleteTopic {}", topicName);
        if (AdminClientUtils.topicExists(adminClient, topicName)) {
            AdminClientUtils.deleteTopic(adminClient, topicName);
            untilAsserted(() -> !AdminClientUtils.topicExists(adminClient, topicName));
        }
        logger.debug("deleteTopic {} finished", topicName);
    }

    private void createTopic(String topicName, int numPartitions) {
        NewTopic topic = new NewTopic(topicName, numPartitions, TEST_TOPIC_REPLICATION_FACTOR);
        logger.debug("createTopic {}", topicName);

        int retries = RETRIES_CNT;
        while (retries > 0) {
            try {
                AdminClientUtils.createTopic(adminClient, topic);
                retries = 0;
            } catch (TopicExistsException e) {
                logger.warn("error while creating topic {}", topicName, e);
                retries--;
                if (retries == 0) {
                    throw e;
                }
                unchecked(() -> Thread.sleep(POLL_INTERVAL.toMillis()));
            }
        }

        untilAsserted(() -> AdminClientUtils.topicExists(adminClient, topicName));
        logger.debug("createTopic {} finished", topicName);
    }

    public void deleteConsumerGroup(String consumerGroup) {
        logger.debug("deleteConsumerGroup {}", consumerGroup);
        if (consumerGroupExists(consumerGroup)) {
            AdminClientUtils.deleteConsumerGroup(adminClient, consumerGroup);
            untilAsserted(() -> !consumerGroupExists(consumerGroup));
        }
        logger.debug("deleteConsumerGroup {} finished", consumerGroup);
    }

    public boolean consumerGroupExists(String consumerGroup) {
        return AdminClientUtils.consumerGroupExists(adminClient, consumerGroup)
                && AdminClientUtils.describeConsumerGroup(adminClient, consumerGroup).state() != ConsumerGroupState.DEAD;
    }

    public String topicName(String testName) {
        return format(testTopicNamePattern, testName);
    }

    public String consumerGroupName(String testName) {
        return format(testConsumerGroupNamePattern, testName);
    }

    public void verifyCommit(String topicName, String groupName, int partition, long expectedOffset) {
        Validate.isTrue(partition >= 0, "negative partition");
        Validate.isTrue(expectedOffset >= 0, "negative expectedOffset");

        untilAsserted(() -> {
            Map<TopicPartition, OffsetAndMetadata> offsets = AdminClientUtils.listConsumerGroupOffsets(
                    adminClient,
                    groupName
            );

            long actualOffset = 0;
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
                TopicPartition actualPartition = entry.getKey();
                boolean isSamePartition = topicName.equals(actualPartition.topic())
                        && partition == actualPartition.partition();
                if (isSamePartition && entry.getValue() != null) {
                    actualOffset = entry.getValue().offset();
                    break;
                }
            }

            return expectedOffset == actualOffset;
        });
    }

    public Map<Integer, PartitionOffsets> resolveTopicOffsets(String topicName) {
        Map<String, TopicDescription> topicDescription = unchecked(() -> adminClient.describeTopics(singleton(topicName)).all().get());

        Map<Integer, Long> earliestOffsets = unchecked(() -> resolveTopicOffsets(topicDescription, new OffsetSpec.EarliestSpec()));
        Map<Integer, Long> latestOffsets = unchecked(() -> resolveTopicOffsets(topicDescription, new OffsetSpec.LatestSpec()));

        Map<Integer, PartitionOffsets> offsets = new HashMap<>();

        earliestOffsets.forEach(
                (partition, earliestOffset) -> offsets.put(
                        partition,
                        new PartitionOffsets(
                                earliestOffset,
                                latestOffsets.get(partition)
                        )
                )
        );

        return offsets;
    }

    private Map<Integer, Long> resolveTopicOffsets(
            Map<String, TopicDescription> topicDescription,
            OffsetSpec spec
    ) throws Exception {
        Map<TopicPartition, OffsetSpec> offsetSpecs = new HashMap<>();
        topicDescription.forEach(
                (topic, partitions) -> partitions.partitions()
                        .forEach(partition -> offsetSpecs.put(
                                new TopicPartition(topic, partition.partition()),
                                spec
                        ))
        );
        Map<Integer, Long> offsets = new HashMap<>();
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> listOffsetsRes = adminClient.listOffsets(offsetSpecs).all().get();
        listOffsetsRes.forEach((k, v) -> offsets.put(k.partition(), v.offset()));
        return offsets;
    }

    private void untilAsserted(Callable<Boolean> callable) {
        long startNanos = System.nanoTime();
        long finishNanos = startNanos + UNTIL_DURATION.toNanos();
        while (System.nanoTime() < finishNanos) {
            if (unchecked(callable)) {
                return;
            }
            unchecked(() -> Thread.sleep(POLL_INTERVAL.toMillis()));
        }
        throw new IllegalStateException(format("assertion still 'false' after timeout %s sec", UNTIL_DURATION.toSeconds()));
    }

    private <T> T unchecked(Callable<T> callable) {
        try {
            return callable.call();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void unchecked(ThrowableRunnable runnable) {
        try {
            runnable.run();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private interface ThrowableRunnable {
        void run() throws Exception;
    }
}
