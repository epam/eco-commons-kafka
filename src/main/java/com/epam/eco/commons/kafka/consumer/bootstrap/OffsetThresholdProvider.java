package com.epam.eco.commons.kafka.consumer.bootstrap;

import java.util.Collection;
import java.util.Map;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

public interface OffsetThresholdProvider {

    Map<TopicPartition, Long> getOffsetThreshold(
            KafkaConsumer<?,?> consumer,
            Collection<TopicPartition> partitions
    );

}
