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
import java.util.List;

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
public class PartitionRecordFetchResultTest {

    @Test
    public void testEmptyResultHasExpectedValues() throws Exception {
        PartitionRecordFetchResult<String, String> result =
                PartitionRecordFetchResult.<String, String>builder().
                partition(new TopicPartition("topic", 0)).
                partitionOffsets(OffsetRange.with(0, 0, false)).
                scannedOffsets(OffsetRange.with(0, 0, false)).
                build();

        Assertions.assertNotNull(result);
        Assertions.assertNotNull(result.getPartition());
        Assertions.assertNotNull(result.getPartitionOffsets());
        Assertions.assertNotNull(result.getScannedOffsets());
        Assertions.assertNotNull(result.getRecords());
        Assertions.assertTrue(result.getRecords().isEmpty());
    }

    @Test
    public void testNonEmptyResultHasExpectedValuesAndIsIterable() throws Exception {
        PartitionRecordFetchResult<String, String> result =
                PartitionRecordFetchResult.<String, String>builder().
                partition(new TopicPartition("topic", 0)).
                partitionOffsets(OffsetRange.with(0, 0, false)).
                scannedOffsets(OffsetRange.with(0, 0, false)).
                addRecord(createTestRecord()).
                addRecord(createTestRecord()).
                addRecord(createTestRecord()).
                addRecord(createTestRecord()).
                addRecord(createTestRecord()).
                addRecords(createTestRecords(10)).
                build();

        Assertions.assertNotNull(result);
        Assertions.assertNotNull(result.getPartition());
        Assertions.assertNotNull(result.getPartitionOffsets());
        Assertions.assertNotNull(result.getScannedOffsets());
        Assertions.assertNotNull(result.getRecords());
        Assertions.assertEquals(15, result.getRecords().size());
        for (ConsumerRecord<String, String> record : result) {
            Assertions.assertNotNull(record);
        }
        for (ConsumerRecord<String, String> record : result.getRecords()) {
            Assertions.assertNotNull(record);
        }
    }

    @Test
    public void testCreationFailsOnMissingArguments1() throws Exception {
        Assertions.assertThrows(Exception.class, () -> {
            PartitionRecordFetchResult.<String, String> builder().build();
        });
    }

    @Test
    public void testCreationFailsOnMissingArguments2() throws Exception {
        Assertions.assertThrows(Exception.class, () -> {
            PartitionRecordFetchResult.<String, String> builder().partition(new TopicPartition("topic", 0)).build();
        });
    }

    @Test
    public void testCreationFailsOnMissingArguments3() throws Exception {
        Assertions.assertThrows(Exception.class, () -> {
            PartitionRecordFetchResult.<String, String> builder().partition(
                    new TopicPartition("topic", 0)).partitionOffsets(OffsetRange.with(0, 0, false)).build();
        });
    }

    @Test
    public void testCreationFailsOnInvalidRecordCollection() throws Exception {
        Assertions.assertThrows(Exception.class, () -> {
            List<ConsumerRecord<String, String>> records = createTestRecords(1);
            records.add(null);
            PartitionRecordFetchResult.<String, String> builder().partition(
                    new TopicPartition("topic", 0)).partitionOffsets(OffsetRange.with(0, 0, false)).scannedOffsets(
                    OffsetRange.with(0, 0, false)).addRecords(records).build();
        });
    }

    @Test
    public void testSerializedToJsonAndBack() throws Exception {
        PartitionRecordFetchResult<String, String> origin =
                PartitionRecordFetchResult.<String, String>builder().
                        partition(new TopicPartition("topic", 0)).
                        partitionOffsets(OffsetRange.with(0, 0, false)).
                        scannedOffsets(OffsetRange.with(0, 0, false)).
                        addRecords(createTestRecords(0)).
                        build();

        ObjectMapper mapper = TestObjectMapperSingleton.INSTANCE;

        String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(origin);
        Assertions.assertNotNull(json);

        PartitionRecordFetchResult<String, String> deserialized = mapper.readValue(
                json,
                new TypeReference<PartitionRecordFetchResult<String, String>>(){});
        Assertions.assertNotNull(deserialized);
        Assertions.assertEquals(deserialized, origin);
    }

    private ConsumerRecord<String, String> createTestRecord() {
        return new ConsumerRecord<>("topic", 0, 0, "key", "value");
    }

    private List<ConsumerRecord<String, String>> createTestRecords(int numberOfRecords) {
        List<ConsumerRecord<String, String>> records = new ArrayList<>(numberOfRecords);
        for (int i = 0; i < numberOfRecords; i++) {
            records.add(createTestRecord());
        }
        return records;
    }

}
