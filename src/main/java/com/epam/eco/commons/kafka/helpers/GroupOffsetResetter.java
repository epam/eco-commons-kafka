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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import com.epam.eco.commons.kafka.KafkaUtils;
import com.epam.eco.commons.kafka.OffsetRange;
import com.epam.eco.commons.kafka.config.ConsumerConfigBuilder;

/**
 * @author Andrei_Tytsik
 */
public class GroupOffsetResetter {

    private final Map<String, Object> consumerConfig;

    private GroupOffsetResetter(
            String bootstrapServers,
            Map<String, Object> consumerConfig) {
        this.consumerConfig = ConsumerConfigBuilder.
                with(consumerConfig).
                bootstrapServers(bootstrapServers).
                minRequiredConfigs().
                enableAutoCommitDisabled().
                clientIdRandom().
                build();
    }

    public static GroupOffsetResetter with(Map<String, Object> consumerConfig) {
        return new GroupOffsetResetter(null, consumerConfig);
    }

    public static GroupOffsetResetter with(String bootstrapServers) {
        return new GroupOffsetResetter(bootstrapServers, null);
    }

    public void resetToEarliest(String groupName, String[] topicNames) {
        resetToEarliestLatest(groupName, topicNames, true);
    }

    public void resetToEarliest(String groupName, TopicPartition[] partitions) {
        resetToEarliestLatest(groupName, partitions, true);
    }

    public void resetToLatest(String groupName, String[] topicNames) {
        resetToEarliestLatest(groupName, topicNames, false);
    }

    public void resetToLatest(String groupName, TopicPartition[] partitions) {
        resetToEarliestLatest(groupName, partitions, false);
    }

    public void reset(String groupName, Map<TopicPartition, Long> offsets) {
        Validate.notNull(groupName, "Group name is null");
        validateOffsets(offsets);

        Map<String, Object> consumerConfig =
                ConsumerConfigBuilder.with(this.consumerConfig).groupId(groupName).build();
        try (KafkaConsumer<?, ?> consumer = new KafkaConsumer<>(consumerConfig)) {
            doReset(consumer, offsets);
        }
    }

    private void resetToEarliestLatest(String groupName, String[] topicNames, boolean earliestLatest) {
        Validate.notNull(groupName, "Group name is null");
        Validate.notEmpty(topicNames, "Collection of topic names is null or empty");
        Validate.noNullElements(topicNames, "Collection of topic names contains null elements");

        Map<String, Object> consumerConfig =
                ConsumerConfigBuilder.with(this.consumerConfig).groupId(groupName).build();
        try (KafkaConsumer<?, ?> consumer = new KafkaConsumer<>(consumerConfig)) {
            TopicPartition[] partitions = KafkaUtils.getTopicPartitionsAsArray(consumer, topicNames);
            Map<TopicPartition, Long> offsets = new HashMap<>();
            for (TopicPartition partition : partitions) {
                if (earliestLatest) {
                    offsets.put(partition, 0L);
                } else {
                    offsets.put(partition, Long.MAX_VALUE);
                }
            }
            doReset(consumer, offsets);
        }
    }

    private void resetToEarliestLatest(String groupName, TopicPartition[] partitions, boolean earliestLatest) {
        Validate.notNull(groupName, "Group name is null");
        Validate.notEmpty(partitions, "Collection of partitions is null or empty");
        Validate.noNullElements(partitions, "Collection of partitions contains null elements");

        Map<String, Object> consumerConfig =
                ConsumerConfigBuilder.with(this.consumerConfig).groupId(groupName).build();
        try (KafkaConsumer<?, ?> consumer = new KafkaConsumer<>(consumerConfig)) {
            Map<TopicPartition, Long> offsets = new HashMap<>();
            for (TopicPartition partition : partitions) {
                if (earliestLatest) {
                    offsets.put(partition, 0L);
                } else {
                    offsets.put(partition, Long.MAX_VALUE);
                }
            }
            doReset(consumer, offsets);
        }
    }

    private void validateOffsets(Map<TopicPartition, Long> offsets) {
        Validate.notEmpty(offsets, "Collection of partition offsets is null or empty");
        Validate.noNullElements(offsets.keySet(),
                "Collection of partition offset keys contains null elements");
        Validate.noNullElements(offsets.values(),
                "Collection of partition offset values contains null elements");

        offsets.forEach((key, value) -> {
            if (value < 0) {
                throw new IllegalArgumentException(
                        String.format("Offset for %s is invalid: %s", key, value));
            }
        });
    }

    private void doReset(Consumer<?, ?> consumer, Map<TopicPartition, Long> offsets) {
        Map<TopicPartition, OffsetRange> offsetRanges = fetchOffsetRanges(offsets.keySet());

        offsets = filterOutUselessAndAdjustOutOfRangeOffsets(offsets, offsetRanges);
        if (offsets.isEmpty()) {
            return;
        }

        consumer.assign(new ArrayList<>(offsets.keySet()));

        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
        for (Entry<TopicPartition, Long> entry : offsets.entrySet()) {
            TopicPartition partition = entry.getKey();
            Long offset = entry.getValue();

            OffsetAndMetadata meta = null;
            try {
                meta = consumer.committed(partition);
            } catch (Exception ex) {
                // ignore
            }
            if (meta != null) {
                offsetsToCommit.put(partition, new OffsetAndMetadata(offset));
            }
        }
        if (!offsetsToCommit.isEmpty()) {
            consumer.commitSync(offsetsToCommit);
        }
    }

    private Map<TopicPartition, OffsetRange> fetchOffsetRanges(Collection<TopicPartition> partitions) {
        return TopicOffsetFetcher.with(consumerConfig).fetchForPartitions(partitions);
    }

    private Map<TopicPartition, Long> filterOutUselessAndAdjustOutOfRangeOffsets(
            Map<TopicPartition, Long> offsets,
            Map<TopicPartition, OffsetRange> offsetRanges) {
        Set<Entry<TopicPartition, Long>> entries = offsets.entrySet();
        Map<TopicPartition, Long> resultOffsets = new HashMap<>((int) (entries.size() / 0.75));
        for (Entry<TopicPartition, Long> entry : entries) {
            TopicPartition partition = entry.getKey();
            long offset = entry.getValue();

            OffsetRange range = offsetRanges.get(partition);
            if (range == null) {
                continue;
            }

            offset = offset < range.getSmallest() ? range.getSmallest() : offset;
            offset = offset > range.getLargest() + 1 ? range.getLargest() + 1 : offset;

            resultOffsets.put(partition, offset);
        }
        return resultOffsets;
    }

}
