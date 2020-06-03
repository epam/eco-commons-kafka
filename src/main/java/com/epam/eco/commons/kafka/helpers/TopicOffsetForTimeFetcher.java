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
package com.epam.eco.commons.kafka.helpers;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;

import com.epam.eco.commons.kafka.KafkaUtils;
import com.epam.eco.commons.kafka.config.ConsumerConfigBuilder;

public class TopicOffsetForTimeFetcher {

    private final Map<String, Object> consumerConfig;

    private TopicOffsetForTimeFetcher(String bootstrapServers, Map<String, Object> consumerConfig) {
        this.consumerConfig = ConsumerConfigBuilder.
                with(consumerConfig).
                bootstrapServers(bootstrapServers).
                minRequiredConfigs().
                enableAutoCommitDisabled().
                clientIdRandom().
                build();
    }

    public static TopicOffsetForTimeFetcher with(Map<String, Object> consumerConfig) {
        return new TopicOffsetForTimeFetcher(null, consumerConfig);
    }

    public static TopicOffsetForTimeFetcher with(String bootstrapServers) {
        return new TopicOffsetForTimeFetcher(bootstrapServers, null);
    }

    public Map<TopicPartition, Long> fetchForPartition(TopicPartition topicPartition, Long timestamp) {
        return fetchForPartitions(
                topicPartition != null ? Collections.singletonList(topicPartition) : null, timestamp);
    }

    public Map<TopicPartition, Long> fetchForPartitions(Map<TopicPartition, Long> topicPartitionTimes) {
        Validate.notEmpty(topicPartitionTimes, "Map of partition timestamps is null or empty");
        Validate.noNullElements(topicPartitionTimes.keySet(), "Collection of partitions null elements");
        Validate.noNullElements(topicPartitionTimes.values(), "Collection of timestamps contains null elements");

        try (KafkaConsumer<?, ?> consumer = new KafkaConsumer<>(consumerConfig)) {
            return doFetch(consumer, topicPartitionTimes);
        }
    }

    public Map<TopicPartition, Long> fetchForPartitions(Collection<TopicPartition> topicPartitions,
                                                               Long timestamp) {
        Validate.notEmpty(topicPartitions, "Collection of partitions is null or empty");
        Validate.noNullElements(topicPartitions, "Collection of partitions contains null elements");
        Validate.notNull(timestamp, "Timestamp is null or empty");

        try (KafkaConsumer<?, ?> consumer = new KafkaConsumer<>(consumerConfig)) {
            return doFetch(consumer, topicPartitions, timestamp);
        }
    }

    public Map<TopicPartition, Long> fetchForTopic(String topicName, Long timestamp) {
        return fetchForTopics(
                topicName != null ? Collections.singletonList(topicName) : null, timestamp);
    }

    public Map<TopicPartition, Long> fetchForTopics(Collection<String> topicNames, Long timestamp) {
        Validate.notEmpty(topicNames, "Collection of topic names is null or empty");
        Validate.noNullElements(topicNames, "Collection of topic names contains null elements");
        Validate.notNull(timestamp, "Timestamp is null or empty");

        try (KafkaConsumer<?, ?> consumer = new KafkaConsumer<>(consumerConfig)) {
            List<TopicPartition> partitions =
                    KafkaUtils.getTopicPartitionsAsList(consumer, topicNames);
            return doFetch(consumer, partitions, timestamp);
        }
    }

    private static Map<TopicPartition, Long> doFetch(
            Consumer<?, ?> consumer, Collection<TopicPartition> partitions, Long timestamp) {

        Map<TopicPartition, Long> partitionTimestamps = partitions.stream()
                .collect(Collectors.toMap(Function.identity(), partition -> timestamp));

        return doFetch(consumer, partitionTimestamps);
    }

    private static Map<TopicPartition, Long> doFetch(Consumer<?, ?> consumer,
                                                     Map<TopicPartition, Long> partitionTimestamps) {
        Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes = consumer.offsetsForTimes(partitionTimestamps);

        return offsetsForTimes.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry ->
                        entry.getValue() == null ? null : entry.getValue().offset()));
    }
}
