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

import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.AbstractIterator;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import com.epam.eco.commons.kafka.TopicPartitionComparator;

/**
 * @author Andrei_Tytsik
 */
public class RecordFetchResult<K, V> implements Iterable<ConsumerRecord<K, V>> {

    @SuppressWarnings("rawtypes")
    private static final RecordFetchResult EMPTY_RESULT = new RecordFetchResult<>(Collections.emptyMap());

    private final Map<TopicPartition, PartitionRecordFetchResult<K, V>> results;

    public RecordFetchResult(
            @JsonProperty("results") Map<TopicPartition, PartitionRecordFetchResult<K, V>> results) {
        Validate.notNull(results, "Collection of results is null");
        Validate.noNullElements(results.keySet(),
                "Collection of result keys contains null elements");
        Validate.noNullElements(results.values(),
                "Collection of result values contains null elements");

        this.results = Collections.unmodifiableMap(results);
    }

    public Map<TopicPartition, PartitionRecordFetchResult<K, V>> getResults() {
        return results;
    }

    @JsonIgnore
    public PartitionRecordFetchResult<K, V> getPerPartitionResult(
            TopicPartition partition) {
        return results.get(partition);
    }

    @JsonIgnore
    public List<PartitionRecordFetchResult<K, V>> getPerPartitionResults() {
        return new ArrayList<>(results.values());
    }

    @JsonIgnore
    public List<ConsumerRecord<K, V>> getRecords(TopicPartition partition) {
        PartitionRecordFetchResult<K, V> result = results.get(partition);
        if (result == null) {
            return Collections.emptyList();
        } else {
            return result.getRecords();
        }
    }

    @JsonIgnore
    public List<ConsumerRecord<K, V>> getRecords(String topic) {
        Validate.notNull(topic, "Topic is null");

        List<ConsumerRecord<K, V>> records = new ArrayList<>();
        for (ConsumerRecord<K, V> record : this) {
            if (record.topic().equals(topic)) {
                records.add(record);
            }
        }
        return records;
    }

    @JsonIgnore
    public List<ConsumerRecord<K, V>> getRecords() {
        List<ConsumerRecord<K, V>> records = new ArrayList<>();
        for (ConsumerRecord<K, V> record : this) {
            records.add(record);
        }
        return records;
    }

    @JsonIgnore
    public List<TopicPartition> getPartitions() {
        return results.keySet().stream().
                sorted(TopicPartitionComparator.INSTANCE).
                collect(Collectors.toList());
    }

    public int count() {
        return results.values().stream().
                map(PartitionRecordFetchResult::getRecords).
                mapToInt(List::size).
                sum();
    }

    @JsonIgnore
    public boolean isEmpty() {
        return count() == 0;
    }

    @Override
    public Iterator<ConsumerRecord<K, V>> iterator() {
        return new ConcatenatedIterable<>(results.values()).iterator();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RecordFetchResult<?, ?> that = (RecordFetchResult<?, ?>) o;
        return Objects.equals(results, that.results);
    }

    @Override
    public int hashCode() {
        return Objects.hash(results);
    }

    @Override
    public String toString() {
        return "RecordFetchResult{" +
                "results=" + results +
                '}';
    }

    private static class ConcatenatedIterable<K, V> implements Iterable<ConsumerRecord<K, V>> {

        private final Iterable<? extends Iterable<ConsumerRecord<K, V>>> iterables;

        public ConcatenatedIterable(Iterable<? extends Iterable<ConsumerRecord<K, V>>> iterables) {
            this.iterables = iterables;
        }

        @Override
        public Iterator<ConsumerRecord<K, V>> iterator() {
            return new AbstractIterator<ConsumerRecord<K, V>>() {
                Iterator<? extends Iterable<ConsumerRecord<K, V>>> iters = iterables.iterator();
                Iterator<ConsumerRecord<K, V>> current;
                public ConsumerRecord<K, V> makeNext() {
                    if (current == null || !current.hasNext()) {
                        if (iters.hasNext()) {
                            current = iters.next().iterator();
                            return makeNext();
                        } else {
                            return allDone();
                        }
                    }
                    return current.next();
                }
            };
        }

    }

    @SuppressWarnings("unchecked")
    public static final <K, V> RecordFetchResult<K, V> emptyResult() {
        return EMPTY_RESULT;
    }

    public static <K, V> Builder<K, V> builder() {
        return new Builder<>();
    }

    public static class Builder<K, V> {

        private final Map<TopicPartition, PartitionRecordFetchResult<K, V>> results = new HashMap<>();

        public Builder<K, V> result(PartitionRecordFetchResult<K, V> result) {
            Validate.notNull(result, "Result is null");

            results.put(result.getPartition(), result);
            return this;
        }
        public RecordFetchResult<K, V> build() {
            return new RecordFetchResult<>(results);
        }

    }

}
