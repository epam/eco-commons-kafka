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
package com.epam.eco.commons.kafka.consumer.advanced;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author Andrei_Tytsik
 */
public class RecordBatchIteratorTest {

    @Test
    public void testOffsestBuiltFromCommitPosition() throws Exception {
        TopicPartition partition = new TopicPartition("topic", 0);
        List<ConsumerRecord<String, String>> partitionRecords = createTestRecords(partition, 100);
        ConsumerRecords<String, String> records =
                new ConsumerRecords<>(Collections.singletonMap(partition, partitionRecords));

        ConsumerRecord<String, String> recordToCommitAfter =
                partitionRecords.get(new Random().nextInt(partitionRecords.size()));

        DefaultRecordBatchIterator<String, String> iterator = new DefaultRecordBatchIterator<>(records);
        for (ConsumerRecord<String, String> record : iterator) {
            if (record.equals(recordToCommitAfter)) {
                iterator.updateCommitPosition();
            }
        }

        Assertions.assertEquals(
                partitionRecords.indexOf(recordToCommitAfter) + 1,
                iterator.countRecordsToCommit());
        Assertions.assertEquals(
                Collections.singletonMap(partition, recordToCommitAfter.offset() + 1),
                iterator.buildOffsetsToCommit());

        iterator.resetCommitPosition();

        Assertions.assertEquals(0, iterator.countRecordsToCommit());
        Assertions.assertEquals(Collections.emptyMap(), iterator.buildOffsetsToCommit());
    }

    private List<ConsumerRecord<String, String>> createTestRecords(
            TopicPartition partition,
            int numberOfRecords) {
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        for (int i = 0; i < numberOfRecords; i++) {
            records.add(new ConsumerRecord<>(
                    partition.topic(),
                    partition.partition(),
                    i,
                    null,
                    null));
        }
        return records;
    }

}
