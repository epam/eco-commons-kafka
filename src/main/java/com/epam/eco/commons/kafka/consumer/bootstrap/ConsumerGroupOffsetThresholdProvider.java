package com.epam.eco.commons.kafka.consumer.bootstrap;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.epam.eco.commons.kafka.AdminClientUtils;
import com.epam.eco.commons.kafka.KafkaUtils;
import com.epam.eco.commons.kafka.OffsetRange;
import com.epam.eco.commons.kafka.helpers.TopicOffsetRangeFetcher;

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
     * When some partition does not yet have offset for the configured consumer group
     * or partition is empty, then it is ignored. I.e. empty map is returned for not existing consumer group.
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

        Map<TopicPartition, Long> result = new HashMap<>();
        partitions.forEach(partition -> {
            if (!topic.equals(partition.topic())) {
                throw new IllegalArgumentException(
                        "Partition '%s' is not part of topic '%s'.".formatted(
                                topic,
                                partition
                        )
                );
            }

            Long offset = offsets.get(partition);
            if (offset != null) {
                result.put(partition, offset);
            }
        });

        return result;
    }

    private void fetchOffsetsIfNeeded(KafkaConsumer<?, ?> consumer) {
        if (offsets != null) {
            return;
        }

        if (AdminClientUtils.consumerGroupExists(adminClientProperties, consumerGroup)) {
            List<TopicPartition> partitions = KafkaUtils.getTopicPartitionsAsList(consumer, topic);
            Map<TopicPartition, OffsetRange> partitionRanges = TopicOffsetRangeFetcher.
                    with(consumer).
                    fetchForPartitions(partitions);
            Map<TopicPartition, OffsetAndMetadata> partitionOffsets = AdminClientUtils.listConsumerGroupOffsets(
                    adminClientProperties,
                    consumerGroup
            );

            Map<TopicPartition, Long> result = new HashMap<>();
            // Fix obsolete offsets (out of range) and filter out empty partitions
            partitionOffsets.forEach((partition, meta) -> {
                OffsetRange range = partitionRanges.get(partition);
                if (meta != null && range != null) {
                    if (range.getSize() > 0) {
                        long offset = fixOffsetBoundaries(meta, range);
                        if (offset != -1) {
                            result.put(partition, offset);
                        }
                    } else {
                        log.debug(
                                "Consumer group offset '{}' for partition '{}' does not fit " +
                                        "partition range '{}'. Offset is ignored",
                                meta.offset(),
                                partition,
                                range
                        );
                    }
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
            log.info("Consumer group '{}' does not exist. Thresholds are empty.", consumerGroup);
            offsets = Collections.emptyMap();
        }
    }

    private long fixOffsetBoundaries(OffsetAndMetadata meta, OffsetRange range) {
        if (meta.offset() < range.getSmallest()) {
            return -1;
        }

        long largestConsumerOffset = range.isLargestInclusive() ?
                        range.getLargest() + 1 :
                        range.getLargest();
        if (meta.offset() > largestConsumerOffset) {
            return largestConsumerOffset;
        }

        return meta.offset();
    }

}
