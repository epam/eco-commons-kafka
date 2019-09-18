/*
 * Copyright 2019 EPAM Systems
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
package com.epam.eco.commons.kafka;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Andrei_Tytsik
 */
public abstract class KafkaUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaUtils.class);

    public static final Pattern TOPIC_PARTITION_PATTERN = Pattern.compile("^(.+)-(0|[1-9]\\d*)$");

    private KafkaUtils() {
    }

    public static TopicPartition[] getAssignmentAsArray(KafkaConsumer<?, ?> consumer) {
        return getAssignmentAsList(consumer).stream().
                toArray(TopicPartition[]::new);
    }

    public static List<TopicPartition> getAssignmentAsList(KafkaConsumer<?, ?> consumer) {
        Validate.notNull(consumer, "Consumer is null");

        return new ArrayList<>(consumer.assignment());
    }

    public static TopicPartition[] getTopicPartitionsAsArray(
            KafkaConsumer<?, ?> consumer,
            String ... topicNames) {
        return getTopicPartitionsAsList(consumer, Arrays.asList(topicNames)).stream().
                toArray(TopicPartition[]::new);
    }

    public static List<TopicPartition> getTopicPartitionsAsList(
            KafkaConsumer<?, ?> consumer,
            String ... topicNames) {
        return getTopicPartitionsAsList(consumer, Arrays.asList(topicNames));
    }

    public static TopicPartition[] getTopicPartitionsAsArray(
            KafkaConsumer<?, ?> consumer,
            Collection<String> topicNames) {
        return getTopicPartitionsAsList(consumer, topicNames).stream().
                toArray(TopicPartition[]::new);
    }

    public static List<TopicPartition> getTopicPartitionsAsList(
            KafkaConsumer<?, ?> consumer,
            Collection<String> topicNames) {
        Validate.notNull(consumer, "Consumer is null");
        Validate.notNull(topicNames, "Collection of topic names is null");

        topicNames = topicNames.stream().
                filter(Objects::nonNull).
                collect(Collectors.toSet());

        List<TopicPartition> topicPartitions = new ArrayList<>();
        for (String topicName : topicNames) {
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(topicName);
            if (partitionInfos == null || partitionInfos.isEmpty()) {
                LOGGER.warn("Partitions metadata not found for topic {}", topicName);
                continue;
            }

            topicPartitions.addAll(
                        partitionInfos.stream().
                        map(info -> new TopicPartition(info.topic(), info.partition())).
                        collect(Collectors.toList()));
        }

        return topicPartitions;
    }

    public static TopicPartition[] getTopicPartitionsAsArray(
            KafkaProducer<?, ?> producer,
            String ... topicNames) {
        return getTopicPartitionsAsList(producer, Arrays.asList(topicNames)).stream().
                toArray(TopicPartition[]::new);
    }

    public static List<TopicPartition> getTopicPartitionsAsList(
            KafkaProducer<?, ?> producer,
            String ... topicNames) {
        return getTopicPartitionsAsList(producer, Arrays.asList(topicNames));
    }

    public static TopicPartition[] getTopicPartitionsAsArray(
            KafkaProducer<?, ?> producer,
            Collection<String> topicNames) {
        return getTopicPartitionsAsList(producer, topicNames).stream().
                toArray(TopicPartition[]::new);
    }

    public static List<TopicPartition> getTopicPartitionsAsList(
            KafkaProducer<?, ?> producer,
            Collection<String> topicNames) {
        Validate.notNull(producer, "Producer is null");
        Validate.notNull(topicNames, "Collection of topic names is null");

        topicNames = topicNames.stream().
                filter(Objects::nonNull).
                collect(Collectors.toSet());

        List<TopicPartition> topicPartitions = new ArrayList<>();
        for (String topicName : topicNames) {
            List<PartitionInfo> partitionInfos = producer.partitionsFor(topicName);
            if (partitionInfos == null || partitionInfos.isEmpty()) {
                LOGGER.warn("Partitions metadata not found for topic {}", topicName);
                continue;
            }

            topicPartitions.addAll(
                        partitionInfos.stream().
                        map(info -> new TopicPartition(info.topic(), info.partition())).
                        collect(Collectors.toList()));
        }

        return topicPartitions;
    }

    public static TopicPartition parseTopicPartition(String string) {
        Validate.notBlank(string, "TopicPartition string is blank");

        Matcher matcher = TOPIC_PARTITION_PATTERN.matcher(string);
        if (!matcher.find()) {
            throw new IllegalArgumentException(
                    String.format("Invalid TopicPartition string '%s'", string));
        }

        String topic = matcher.group(1);
        int partition = Integer.parseInt(matcher.group(2));
        return new TopicPartition(
                topic,
                partition);
    }

    public static void closeQuietly(KafkaConsumer<?, ?> consumer) {
        if (consumer == null) {
            return;
        }
        try {
            consumer.close();
        } catch (Exception ex) {
            // ignore
        }
    }

    public static void closeQuietly(KafkaProducer<?, ?> producer) {
        if (producer == null) {
            return;
        }
        try {
            producer.close();
        } catch (Exception ex) {
            // ignore
        }
    }

    public static void closeQuietly(Closeable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception ex) {
            // ignore
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static Map<TopicPartition, Long> extractSmallestOffsets(ConsumerRecords<?, ?> records) {
        Validate.notNull(records, "ConsumerRecords object is null");

        Set<TopicPartition> partitions = records.partitions();
        Map<TopicPartition, Long> smallestOffsets = new HashMap<>((int) (partitions.size() / 0.75));
        for (TopicPartition partition : partitions) {
            List<ConsumerRecord<?, ?>> recordsPerPartition =
                    ((ConsumerRecords)records).records(partition);
            if (recordsPerPartition.isEmpty()) {
                continue;
            }
            ConsumerRecord<?, ?> firstRecordInPartition = recordsPerPartition.get(0);
            smallestOffsets.put(partition, firstRecordInPartition.offset());
        }
        return smallestOffsets;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static Map<TopicPartition, Long> extractLargestOffsets(ConsumerRecords<?, ?> records) {
        Validate.notNull(records, "ConsumerRecords object is null");

        Set<TopicPartition> partitions = records.partitions();
        Map<TopicPartition, Long> largestOffsets = new HashMap<>((int) (partitions.size() / 0.75));
        for (TopicPartition partition : partitions) {
            List<ConsumerRecord<?, ?>> recordsPerPartition =
                    ((ConsumerRecords)records).records(partition);
            if (recordsPerPartition.isEmpty()) {
                continue;
            }
            ConsumerRecord<?, ?> lastRecordInPartition =
                    recordsPerPartition.get(recordsPerPartition.size() - 1);
            largestOffsets.put(partition, lastRecordInPartition.offset());
        }
        return largestOffsets;
    }

    public static Map<TopicPartition, Long> getConsumerPositions(KafkaConsumer<?, ?> consumer) {
        Validate.notNull(consumer, "Consumer is null");

        Set<TopicPartition> partitions = consumer.assignment();
        if (partitions == null || partitions.isEmpty()) {
            return Collections.emptyMap();
        }

        return partitions.stream().
                collect(Collectors.toMap(
                        Function.identity(),
                        partition -> consumer.position(partition)));
    }

    public static List<String> extractTopicNamesAsList(Collection<TopicPartition> partitions) {
        Validate.notNull(partitions, "Collection of partitions is null");

        return partitions.stream().
                filter(Objects::nonNull).
                map(TopicPartition::topic).
                distinct().
                collect(Collectors.toList());
    }

    public static List<String> extractTopicNamesAsSortedList(Collection<TopicPartition> partitions) {
        Validate.notNull(partitions, "Collection of partitions is null");

        return partitions.stream().
                filter(Objects::nonNull).
                map(TopicPartition::topic).
                distinct().
                sorted().
                collect(Collectors.toList());
    }

    public static Set<String> extractTopicNamesAsSet(Collection<TopicPartition> partitions) {
        Validate.notNull(partitions, "Collection of partitions is null");

        return partitions.stream().
                filter(Objects::nonNull).
                map(TopicPartition::topic).
                collect(Collectors.toSet());
    }

    public static Set<String> extractTopicNamesAsSortedSet(Collection<TopicPartition> partitions) {
        Validate.notNull(partitions, "Collection of partitions is null");

        return partitions.stream().
                filter(Objects::nonNull).
                map(TopicPartition::topic).
                collect(Collectors.toSet());
    }

    public static <V> Map<TopicPartition, V> sortedByTopicPartitionKeyMap(Map<TopicPartition, V> map) {
        Validate.notNull(map, "Map is null");

        return map.entrySet().stream().
                collect(
                        Collectors.toMap(
                                Entry::getKey,
                                Entry::getValue,
                                (a, b) -> b,
                                () -> new TreeMap<>(TopicPartitionComparator.INSTANCE)));
    }

    public static long calculateConsumerLag(OffsetRange topicOffsetRange, long consumerOffset) {
        Validate.notNull(topicOffsetRange, "Offset range is null");
        Validate.isTrue(consumerOffset >= 0, "Consumer offset is invalid");

        if (consumerOffset < topicOffsetRange.getSmallest()) {
            return topicOffsetRange.getSize();
        }

        long largestConsumableOffset =
                topicOffsetRange.isLargestInclusive() ?
                topicOffsetRange.getLargest() + 1 :
                topicOffsetRange.getLargest();

        if (consumerOffset > largestConsumableOffset) {
            return -1;
        }

        return largestConsumableOffset - consumerOffset;
    }

}
