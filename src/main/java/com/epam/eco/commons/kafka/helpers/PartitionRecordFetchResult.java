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
package com.epam.eco.commons.kafka.helpers;

import java.util.*;

import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import com.fasterxml.jackson.annotation.JsonProperty;

import com.epam.eco.commons.kafka.OffsetRange;

/**
 * @author Andrei_Tytsik
 */
public class PartitionRecordFetchResult<K, V> implements Iterable<ConsumerRecord<K, V>> {

    private final TopicPartition partition;
    private final List<ConsumerRecord<K, V>> records;
    private final OffsetRange partitionOffsets;
    private final OffsetRange scannedOffsets;

    public PartitionRecordFetchResult(
            @JsonProperty("partition") TopicPartition partition,
            @JsonProperty("records") List<ConsumerRecord<K, V>> records,
            @JsonProperty("partitionOffsets") OffsetRange partitionOffsets,
            @JsonProperty("scannedOffsets") OffsetRange scannedOffsets) {
        Validate.notNull(partition, "Partition is null");
        Validate.notNull(records, "Collection of records is null");
        Validate.noNullElements(records, "Collection of records contains null elements");
        Validate.notNull(partitionOffsets, "Partition offset range is null");
        Validate.notNull(scannedOffsets, "Scanned offset range is null");

        this.partition = partition;
        this.records = Collections.unmodifiableList(records);
        this.partitionOffsets = partitionOffsets;
        this.scannedOffsets = scannedOffsets;
    }

    public TopicPartition getPartition() {
        return partition;
    }
    public List<ConsumerRecord<K, V>> getRecords() {
        return records;
    }
    public OffsetRange getPartitionOffsets() {
        return partitionOffsets;
    }
    public OffsetRange getScannedOffsets() {
        return scannedOffsets;
    }

    @Override
    public Iterator<ConsumerRecord<K, V>> iterator() {
        return records.iterator();
    }

    public static <K, V> Builder<K, V> builder() {
        return new Builder<>();
    }

    public static class Builder<K, V> {

        private TopicPartition partition;
        private final List<ConsumerRecord<K, V>> records = new ArrayList<>();
        private OffsetRange partitionOffsets;
        private OffsetRange scannedOffsets;

        public Builder<K, V> partition(TopicPartition partition) {
            this.partition = partition;
            return this;
        }
        public Builder<K, V> addRecord(ConsumerRecord<K, V> record) {
            this.records.add(record);
            return this;
        }
        public Builder<K, V> addRecords(Collection<ConsumerRecord<K, V>> records) {
            this.records.addAll(records);
            return this;
        }
        public Builder<K, V> partitionOffsets(OffsetRange partitionOffsets) {
            this.partitionOffsets = partitionOffsets;
            return this;
        }
        public Builder<K, V> scannedOffsets(OffsetRange scannedOffsets) {
            this.scannedOffsets = scannedOffsets;
            return this;
        }
        public PartitionRecordFetchResult<K, V> build() {
            return new PartitionRecordFetchResult<>(
                    partition,
                    records,
                    partitionOffsets,
                    scannedOffsets);
        }

    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        PartitionRecordFetchResult<?, ?> that = (PartitionRecordFetchResult<?, ?>) obj;
        return
                Objects.equals(partition, that.partition) &&
                Objects.equals(records, that.records) &&
                Objects.equals(partitionOffsets, that.partitionOffsets) &&
                Objects.equals(scannedOffsets, that.scannedOffsets);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partition, records, partitionOffsets, scannedOffsets);
    }

    @Override
    public String toString() {
        return "PartitionRecordFetchResult{" +
                "partition=" + partition +
                ", records=" + records +
                ", partitionOffsets=" + partitionOffsets +
                ", scannedOffsets=" + scannedOffsets +
                '}';
    }
}
