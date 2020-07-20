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
package com.epam.eco.commons.kafka.consumer.bootstrap;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.time.temporal.UnsupportedTemporalTypeException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;

/**
 * @author Andrei_Tytsik
 */
public class TimestampOffsetInitializer implements OffsetInitializer {

    private final long timestamp;

    public TimestampOffsetInitializer(long timestamp) {
        Validate.isTrue(timestamp > 0, "Timestamp is invalid");

        this.timestamp = timestamp;
    }

    @Override
    public void init(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        Map<TopicPartition, Long> offsets = getOffsetsForTimestamp(consumer, partitions);

        Set<TopicPartition> notReadablePartitions = getNotReadablePartitions(offsets);
        consumer.seekToEnd(notReadablePartitions);

        Map<TopicPartition, Long> readableOffsets = getReadableOffsets(offsets);
        readableOffsets.forEach(consumer::seek);
    }

    private Map<TopicPartition, Long> getOffsetsForTimestamp(
            Consumer<?, ?> consumer,
            Collection<TopicPartition> partitions) {
        Map<TopicPartition, OffsetAndTimestamp> offsetsAndTimestamps = consumer.offsetsForTimes(
                partitions.stream().collect(
                        Collectors.toMap(
                                Function.identity(),
                                partition -> timestamp)));

        Map<TopicPartition, Long> offsets = new HashMap<>();
        offsetsAndTimestamps.forEach((topicPartition, offsetAndTimestamp) -> {
            offsets.put(
                    topicPartition,
                    offsetAndTimestamp != null ? offsetAndTimestamp.offset() : null);
        });

        return offsets;
    }

    private Map<TopicPartition, Long> getReadableOffsets(Map<TopicPartition, Long> offsets) {
        return offsets.entrySet().stream().
                filter(e -> e.getValue() != null).
                collect(Collectors.toMap(
                        e -> e.getKey(),
                        e -> e.getValue()));
    }

    private Set<TopicPartition> getNotReadablePartitions(Map<TopicPartition, Long> offsets) {
        return offsets.entrySet().stream().
                filter(e -> e.getValue() == null).
                map(Entry::getKey).
                collect(Collectors.toSet());
    }

    public static TimestampOffsetInitializer forTimestamp(long timestamp) {
        return new TimestampOffsetInitializer(timestamp);
    }

    public static TimestampOffsetInitializer oneHourBefore() {
        return hoursBefore(1);
    }

    public static TimestampOffsetInitializer twoHoursBefore() {
        return hoursBefore(2);
    }

    public static TimestampOffsetInitializer threeHoursBefore() {
        return hoursBefore(3);
    }

    public static TimestampOffsetInitializer hoursBefore(long hours) {
        return forNowMinus(hours, ChronoUnit.HOURS);
    }

    public static TimestampOffsetInitializer oneDayBefore() {
        return daysBefore(1);
    }

    public static TimestampOffsetInitializer twoDaysBefore() {
        return daysBefore(2);
    }

    public static TimestampOffsetInitializer threeDaysBefore() {
        return daysBefore(3);
    }

    public static TimestampOffsetInitializer daysBefore(long days) {
        return forNowMinus(days, ChronoUnit.DAYS);
    }

    public static TimestampOffsetInitializer oneWeekBefore() {
        return weeksBefore(1);
    }

    public static TimestampOffsetInitializer twoWeeksBefore() {
        return weeksBefore(2);
    }

    public static TimestampOffsetInitializer weeksBefore(long weeks) {
        return forNowMinus(weeks, ChronoUnit.WEEKS);
    }

    public static TimestampOffsetInitializer forNowMinus(long amount, TemporalUnit unit) {
        Validate.isTrue(amount > 0, "Amount is invalid");
        Validate.notNull(unit, "TimeUnit is null");

        if (unit.getDuration().compareTo(ChronoUnit.WEEKS.getDuration()) == 0) {
            amount *= 7;
            unit = ChronoUnit.DAYS;
        } else if (unit.getDuration().compareTo(ChronoUnit.WEEKS.getDuration()) > 0) {
            throw new UnsupportedTemporalTypeException("Unit is not supported: " + unit);
        }

        long timestamp = Instant.now().minus(amount, unit).toEpochMilli();
        return forTimestamp(timestamp);
    }

}
