package com.epam.eco.commons.kafka.consumer.bootstrap;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.epam.eco.commons.kafka.AdminClientUtils;
import com.epam.eco.commons.kafka.KafkaUtils;

/**
 * {@link OffsetThresholdProvider} implementation that fetches threshold offsets for specific consumer
 * group of specific topic. Offsets are lazily loaded on the first call of
 * {@link #getOffsetThreshold(KafkaConsumer, Collection)}, for the next calls of the method, offsets
 * are returned from the cache.
 * <br>
 * <b>Note:</b> this class is not thread safe
 */
public class ConsumerGroupOffsetThresholdProvider implements OffsetThresholdProvider {

    private static final Logger log = LoggerFactory.getLogger(ConsumerGroupOffsetThresholdProvider.class);

    private final Map<String, Object> adminClientProperties;
    private final String topic;
    private final String consumerGroup;

    private Map<TopicPartition, Long> offsets;

    public ConsumerGroupOffsetThresholdProvider(
            String topic,
            String consumerGroup,
            Map<String, Object> adminClientProperties
    ) {
        Validate.notEmpty(adminClientProperties, "adminClientProperties must not be empty");
        Validate.notEmpty(topic, "topic must not be empty");
        Validate.notEmpty(consumerGroup, "consumerGroup must not be empty");

        this.adminClientProperties = Map.copyOf(adminClientProperties);
        this.topic = topic;
        this.consumerGroup = consumerGroup;
    }

    /**
     * Returns consumer group offsets for specific partitions of configured topic.
     * If provided partitions are not part of the topic, then exception is thrown.
     * <br>
     * When consumer group does not exist or some partition does not yet have offset
     * for the configured consumer group, then 0 is returned as threshold.
     *
     * @param consumer consumer
     * @param partitions partitions to get thresholds for
     * @return consumer offsets
     * @throws IllegalArgumentException if provided partitions are not part of configured topic
     */
    @Override
    public Map<TopicPartition, Long> getOffsetThreshold(
            KafkaConsumer<?, ?> consumer,
            Collection<TopicPartition> partitions
    ) {
        fetchOffsetsIfNeeded(consumer);

        partitions.forEach(partition -> {
            if (!offsets.containsKey(partition)) {
                throw new IllegalArgumentException(
                        "Offsets for topic '%s' partition '%s' could not be found.".formatted(
                                topic,
                                partition
                        )
                );
            }
        });

        return partitions.stream()
                .collect(Collectors.toMap(Function.identity(), offsets::get));
    }

    private void fetchOffsetsIfNeeded(KafkaConsumer<?, ?> consumer) {
        if (offsets != null) {
            return;
        }
        List<TopicPartition> partitions = KafkaUtils.getTopicPartitionsAsList(consumer, topic);

        if (AdminClientUtils.consumerGroupExists(adminClientProperties, consumerGroup)) {
            Map<TopicPartition, Long> result = new HashMap<>();

            AdminClientUtils.listConsumerGroupOffsets(
                    adminClientProperties,
                    consumerGroup,
                    partitions.toArray(new TopicPartition[0])
            ).forEach((partition, meta) -> {
                if (meta != null) {
                    result.put(partition, meta.offset());
                } else {
                    result.put(partition, 0L);
                }
            });

            log.info(
                    "Fetched consumer group '{}' offsets for topic '{}': {}",
                    consumerGroup,
                    topic,
                    result
            );
            offsets = result;
        } else {
            log.info("Consumer group '{}' does not exist. Setting thresholds to 0", consumerGroup);
            offsets = partitions.stream()
                    .collect(Collectors.toMap(Function.identity(), partition -> 0L));
        }
    }

}
