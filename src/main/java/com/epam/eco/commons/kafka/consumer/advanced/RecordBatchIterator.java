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

import java.util.Iterator;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

/**
 * @author Andrei_Tytsik
 */
public interface RecordBatchIterator<K, V> extends Iterable<ConsumerRecord<K, V>>, Iterator<ConsumerRecord<K, V>> {
    void updateCommitPosition();
    void resetCommitPosition();
    int countRecordsToCommit();
    Map<TopicPartition, Long> buildOffsetsToCommit();
}
