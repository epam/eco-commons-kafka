package com.epam.eco.commons.kafka.consumer.bootstrap;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import com.epam.eco.commons.kafka.OffsetRange;
import com.epam.eco.commons.kafka.helpers.TopicOffsetRangeFetcher;

public class EndOffsetThresholdProvider implements OffsetThresholdProvider {

    public static final EndOffsetThresholdProvider INSTANCE = new EndOffsetThresholdProvider();

    @Override
    public Map<TopicPartition, Long> getOffsetThreshold(
            KafkaConsumer<?, ?> consumer,
            Collection<TopicPartition> partitions
    ) {
        Map<TopicPartition, OffsetRange> offsets = TopicOffsetRangeFetcher.
                with(consumer).
                fetchForPartitions(partitions);
        return offsets.entrySet().stream().
                filter(e -> e.getValue().getSize() > 0).
                filter(e -> e.getValue().contains(consumer.position(e.getKey()))).
                collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().getLargest()));
    }

}
