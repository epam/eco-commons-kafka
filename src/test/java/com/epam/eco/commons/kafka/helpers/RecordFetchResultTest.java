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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.epam.eco.commons.kafka.OffsetRange;
import com.epam.eco.commons.kafka.util.TestObjectMapperSingleton;

/**
 * @author Andrei_Tytsik
 */
public class RecordFetchResultTest {

    @Test
    public void testEmptyResultHasExpectedValues() throws Exception {
        RecordFetchResult<String, String> result =
                RecordFetchResult.<String, String>builder().
                result(createTestPerPartitionResult("topic", 0, 0)).
                build();

        Assertions.assertNotNull(result);
        Assertions.assertNotNull(result.getPartitions());
        Assertions.assertEquals(1, result.getPartitions().size());
        Assertions.assertNotNull(result.getPerPartitionResult(result.getPartitions().get(0)));
        Assertions.assertEquals(1, result.getPerPartitionResults().size());
        Assertions.assertNotNull(result.getRecords());
        Assertions.assertEquals(0, result.count());
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testNonEmptyResultHasExpectedValuesAndIsIterable() throws Exception {
        RecordFetchResult<String, String> result =
                RecordFetchResult.<String, String>builder().
                result(createTestPerPartitionResult("topic1", 0, 10)).
                result(createTestPerPartitionResult("topic2", 1, 10)).
                result(createTestPerPartitionResult("topic3", 2, 10)).
                build();

        Assertions.assertNotNull(result);

        Assertions.assertNotNull(result.getPartitions());
        Assertions.assertEquals(3, result.getPartitions().size());

        Assertions.assertEquals(3, result.getPerPartitionResults().size());
        Assertions.assertNotNull(result.getPerPartitionResult(result.getPartitions().get(0)));
        Assertions.assertNotNull(result.getPerPartitionResult(result.getPartitions().get(1)));
        Assertions.assertNotNull(result.getPerPartitionResult(result.getPartitions().get(2)));

        Assertions.assertNotNull(result.getRecords());
        Assertions.assertEquals(30, result.getRecords().size());

        Assertions.assertNotNull(result.getRecords("topic1"));
        Assertions.assertEquals(10, result.getRecords("topic1").size());
        Assertions.assertEquals(10, result.getRecords(result.getPartitions().get(0)).size());

        Assertions.assertNotNull(result.getRecords("topic2"));
        Assertions.assertEquals(10, result.getRecords("topic2").size());
        Assertions.assertEquals(10, result.getRecords(result.getPartitions().get(1)).size());

        Assertions.assertNotNull(result.getRecords("topic3"));
        Assertions.assertEquals(10, result.getRecords("topic3").size());
        Assertions.assertEquals(10, result.getRecords(result.getPartitions().get(2)).size());

        Assertions.assertEquals(30, result.count());
        Assertions.assertFalse(result.isEmpty());

        for (ConsumerRecord<String, String> record : result) {
            Assertions.assertNotNull(record);
        }
        for (ConsumerRecord<String, String> record : result.getRecords()) {
            Assertions.assertNotNull(record);
        }
    }

    @Test
    public void testCreationFailsOnNullResultCollection() throws Exception {
        Assertions.assertThrows(Exception.class, () -> new RecordFetchResult<>(null));
    }

    @Test
    public void testCreationFailsOnInvalidResultCollection1() throws Exception {
        Assertions.assertThrows(Exception.class, () -> {
            Map<TopicPartition, PartitionRecordFetchResult<String, String>> results = new HashMap<>();
            results.put(null, createTestPerPartitionResult("topic", 0, 0));
            new RecordFetchResult<>(results);
        });
    }

    @Test
    public void testCreationFailsOnInvalidResultCollection2() throws Exception {
        Assertions.assertThrows(Exception.class, () -> {
            Map<TopicPartition, PartitionRecordFetchResult<String, String>> results = new HashMap<>();
            results.put(new TopicPartition("topic", 0), null);
            new RecordFetchResult<>(results);
        });
    }

    @Test
    public void testSerializedToJsonAndBack() throws Exception {
        RecordFetchResult<String, String> origin =
                RecordFetchResult.<String, String>builder().
                        result(createTestPerPartitionResult("topic", 0, 0)).
                        build();

        ObjectMapper mapper = TestObjectMapperSingleton.INSTANCE;

        String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(origin);
        Assertions.assertNotNull(json);

        RecordFetchResult<String, String> deserialized = mapper.readValue(
                json,
                new TypeReference<RecordFetchResult<String, String>>(){});
        Assertions.assertNotNull(deserialized);
        Assertions.assertEquals(deserialized, origin);
    }

    @SuppressWarnings("unused")
    private ConsumerRecord<String, String> createTestRecord() {
        return new ConsumerRecord<>("topic", 0, 0, "key", "value");
    }

    private PartitionRecordFetchResult<String, String> createTestPerPartitionResult(String topic, int partition, int numberOfRecords) {
        return
                PartitionRecordFetchResult.<String, String>builder().
                    partition(new TopicPartition(topic, partition)).
                    partitionOffsets(OffsetRange.with(0, 0, false)).
                    scannedOffsets(OffsetRange.with(0, 0, false)).
                    addRecords(createTestRecords(topic, partition, numberOfRecords)).
                    build();
    }

    private ConsumerRecord<String, String> createTestRecord(String topic, int partition) {
        return new ConsumerRecord<>(topic, partition, 0, "key", "value");
    }

    private List<ConsumerRecord<String, String>> createTestRecords(String topic, int partition, int numberOfRecords) {
        List<ConsumerRecord<String, String>> records = new ArrayList<>(numberOfRecords);
        for (int i = 0; i < numberOfRecords; i++) {
            records.add(createTestRecord(topic, partition));
        }
        return records;
    }

}
