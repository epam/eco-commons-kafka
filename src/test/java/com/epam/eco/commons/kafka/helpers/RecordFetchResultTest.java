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
import org.junit.Assert;
import org.junit.Test;

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

        Assert.assertNotNull(result);
        Assert.assertNotNull(result.getPartitions());
        Assert.assertEquals(1, result.getPartitions().size());
        Assert.assertNotNull(result.getPerPartitionResult(result.getPartitions().get(0)));
        Assert.assertEquals(1, result.getPerPartitionResults().size());
        Assert.assertNotNull(result.getRecords());
        Assert.assertEquals(0, result.count());
        Assert.assertTrue(result.isEmpty());
    }

    @Test
    public void testNonEmptyResultHasExpectedValuesAndIsIterable() throws Exception {
        RecordFetchResult<String, String> result =
                RecordFetchResult.<String, String>builder().
                result(createTestPerPartitionResult("topic1", 0, 10)).
                result(createTestPerPartitionResult("topic2", 1, 10)).
                result(createTestPerPartitionResult("topic3", 2, 10)).
                build();

        Assert.assertNotNull(result);

        Assert.assertNotNull(result.getPartitions());
        Assert.assertEquals(3, result.getPartitions().size());

        Assert.assertEquals(3, result.getPerPartitionResults().size());
        Assert.assertNotNull(result.getPerPartitionResult(result.getPartitions().get(0)));
        Assert.assertNotNull(result.getPerPartitionResult(result.getPartitions().get(1)));
        Assert.assertNotNull(result.getPerPartitionResult(result.getPartitions().get(2)));

        Assert.assertNotNull(result.getRecords());
        Assert.assertEquals(30, result.getRecords().size());

        Assert.assertNotNull(result.getRecords("topic1"));
        Assert.assertEquals(10, result.getRecords("topic1").size());
        Assert.assertEquals(10, result.getRecords(result.getPartitions().get(0)).size());

        Assert.assertNotNull(result.getRecords("topic2"));
        Assert.assertEquals(10, result.getRecords("topic2").size());
        Assert.assertEquals(10, result.getRecords(result.getPartitions().get(1)).size());

        Assert.assertNotNull(result.getRecords("topic3"));
        Assert.assertEquals(10, result.getRecords("topic3").size());
        Assert.assertEquals(10, result.getRecords(result.getPartitions().get(2)).size());

        Assert.assertEquals(30, result.count());
        Assert.assertFalse(result.isEmpty());

        for (ConsumerRecord<String, String> record : result) {
            Assert.assertNotNull(record);
        }
        for (ConsumerRecord<String, String> record : result.getRecords()) {
            Assert.assertNotNull(record);
        }
    }

    @Test(expected=Exception.class)
    public void testCreationFailsOnNullResultCollection() throws Exception {
        new RecordFetchResult<>(null);
    }

    @Test(expected=Exception.class)
    public void testCreationFailsOnInvalidResultCollection1() throws Exception {
        Map<TopicPartition, PartitionRecordFetchResult<String, String>> results = new HashMap<>();
        results.put(null, createTestPerPartitionResult("topic", 0, 0));
        new RecordFetchResult<>(results);
    }

    @Test(expected=Exception.class)
    public void testCreationFailsOnInvalidResultCollection2() throws Exception {
        Map<TopicPartition, PartitionRecordFetchResult<String, String>> results = new HashMap<>();
        results.put(new TopicPartition("topic", 0), null);
        new RecordFetchResult<>(results);
    }

    @Test
    public void testSerializedToJsonAndBack() throws Exception {
        RecordFetchResult<String, String> origin =
                RecordFetchResult.<String, String>builder().
                        result(createTestPerPartitionResult("topic", 0, 0)).
                        build();

        ObjectMapper mapper = TestObjectMapperSingleton.INSTANCE;

        String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(origin);
        Assert.assertNotNull(json);

        RecordFetchResult<String, String> deserialized = mapper.readValue(
                json,
                new TypeReference<RecordFetchResult<String, String>>(){});
        Assert.assertNotNull(deserialized);
        Assert.assertEquals(deserialized, origin);
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
