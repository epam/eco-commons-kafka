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
package com.epam.eco.commons.kafka.consumer.advanced;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

/**
 * @author Andrei_Tytsik
 */
class DefaultRecordBatchIterator<K, V> implements InternalRecordBatchIterator<K, V> {

    private final ConsumerRecords<K, V> records;
    private final Iterator<ConsumerRecord<K, V>> iterator;
    private final AtomicBoolean running = new AtomicBoolean(true);

    private final List<ConsumerRecord<K, V>> handledRecords = new ArrayList<>();
    private final AtomicInteger commitPosition = new AtomicInteger(0);

    DefaultRecordBatchIterator(ConsumerRecords<K, V> records) {
        Validate.notNull(records, "Records object is null");

        this.records = records;
        iterator = records.iterator();
    }

    @Override
    public Iterator<ConsumerRecord<K, V>> iterator() {
        return this;
    }

    @Override
    public void updateCommitPosition() {
        commitPosition.set(handledRecords.size());
    }

    @Override
    public void resetCommitPosition() {
        commitPosition.set(0);
    }

    @Override
    public boolean hasNext() {
        return running.get() && iterator.hasNext();
    }

    @Override
    public ConsumerRecord<K, V> next() {
        ConsumerRecord<K, V> record = iterator.next();
        handledRecords.add(record);
        return record;
    }

    @Override
    public int countRecordsToCommit() {
        return commitPosition.get();
    }

    @Override
    public Map<TopicPartition, Long> buildOffsetsToCommit() {
        if (commitPosition.get() == 0) {
            return Collections.emptyMap();
        }

        Map<TopicPartition, Long> offsetsToCommit = new HashMap<>((int) (commitPosition.get() / 0.75));
        for (int i = 0; i < commitPosition.get(); i++) {
            ConsumerRecord<K, V> record = handledRecords.get(i);
            offsetsToCommit.put(
                    new TopicPartition(record.topic(), record.partition()),
                    record.offset() + 1);
        }
        return offsetsToCommit;
    }

    @Override
    public ConsumerRecords<K, V> getRecords() {
        return records;
    }

    @Override
    public void interrupt() {
        running.set(false);
    }

}
