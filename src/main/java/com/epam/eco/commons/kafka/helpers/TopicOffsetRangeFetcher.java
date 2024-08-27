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
package com.epam.eco.commons.kafka.helpers;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import com.epam.eco.commons.kafka.KafkaUtils;
import com.epam.eco.commons.kafka.OffsetRange;
import com.epam.eco.commons.kafka.TopicPartitionComparator;
import com.epam.eco.commons.kafka.config.ConsumerConfigBuilder;

/**
 * @author Andrei_Tytsik
 */
public class TopicOffsetRangeFetcher {

    protected final Map<String, Object> consumerConfig;
    protected final KafkaConsumer<?,?> consumer;

    protected TopicOffsetRangeFetcher(String bootstrapServers, Map<String, Object> consumerConfig) {
        this.consumerConfig = ConsumerConfigBuilder.
                with(consumerConfig).
                bootstrapServers(bootstrapServers).
                minRequiredConfigs().
                enableAutoCommitDisabled().
                clientIdRandom().
                build();
        this.consumer = null;
    }

    protected TopicOffsetRangeFetcher(KafkaConsumer<?,?> consumer) {
        this.consumerConfig = null;
        this.consumer = consumer;
    }

    public static TopicOffsetRangeFetcher with(Map<String, Object> consumerConfig) {
        return new TopicOffsetRangeFetcher(null, consumerConfig);
    }

    public static TopicOffsetRangeFetcher with(String bootstrapServers) {
        return new TopicOffsetRangeFetcher(bootstrapServers, null);
    }

    public static TopicOffsetRangeFetcher with(KafkaConsumer<?,?> consumer) {
        return new TopicOffsetRangeFetcher(consumer);
    }

    public Map<TopicPartition, OffsetRange> fetchForPartitions(TopicPartition ... partitions) {
        return fetchForPartitions(
                partitions != null ? Arrays.asList(partitions) : null);
    }

    public Map<TopicPartition, OffsetRange> fetchForPartitions(Collection<TopicPartition> partitions) {
        Validate.notEmpty(partitions, "Collection of partitions is null or empty");
        Validate.noNullElements(partitions, "Collection of partitions contains null elements");

        if (consumer != null) {
            return doFetch(consumer, partitions);
        } else {
            try (KafkaConsumer<?, ?> localConsumer = new KafkaConsumer<>(consumerConfig)) {
                return doFetch(localConsumer, partitions);
            }
        }
    }

    public Map<TopicPartition, OffsetRange> fetchForTopics(String ... topicNames) {
        return fetchForTopics(
                topicNames != null ? Arrays.asList(topicNames) : null);
    }

    public Map<TopicPartition, OffsetRange> fetchForTopics(Collection<String> topicNames) {
        Validate.notEmpty(topicNames, "Collection of topic names is null or empty");
        Validate.noNullElements(topicNames, "Collection of topic names contains null elements");

        if (consumer != null) {
            return doFetchForTopics(consumer, topicNames);
        } else {
            try (KafkaConsumer<?, ?> localConsumer = new KafkaConsumer<>(consumerConfig)) {
                return doFetchForTopics(localConsumer, topicNames);
            }
        }
    }

    private Map<TopicPartition, OffsetRange> doFetchForTopics(
            KafkaConsumer<?, ?> consumer,
            Collection<String> topicNames
    ) {
        List<TopicPartition> partitions =
                KafkaUtils.getTopicPartitionsAsList(consumer, topicNames);
        return doFetch(consumer, partitions);
    }

    /**
     * Offsets ranges might not be accurate as Kafka doesn't guarantee consecutive offsets (there might be
     * transaction markers which aren't consumable and look like gaps on client side).
     */
    protected static Map<TopicPartition, OffsetRange> doFetch(
            Consumer<?, ?> consumer,
            Collection<TopicPartition> partitions) {
        Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(partitions);
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);

        Map<TopicPartition, OffsetRange> offsets = new TreeMap<>(TopicPartitionComparator.INSTANCE);
        for (TopicPartition partition : partitions) {
            long offsetAtBeginning = beginningOffsets.get(partition);
            long offsetAtEnd = endOffsets.get(partition);
            offsets.put(
                    partition,
                    OffsetRange.with(
                            offsetAtBeginning,
                            offsetAtEnd > offsetAtBeginning,
                            offsetAtEnd > offsetAtBeginning ? offsetAtEnd - 1 : offsetAtEnd,
                            offsetAtEnd > offsetAtBeginning));
        }
        return offsets;
    }

}

