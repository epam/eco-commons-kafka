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
package com.epam.eco.commons.kafka.serde.jackson;

import java.util.Arrays;
import java.util.Date;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.epam.eco.commons.kafka.util.TestObjectMapperSingleton;

/**
 * @author Raman_Babich
 */
public class ConsumerRecordSerdeTest {

    @SuppressWarnings("deprecation")
    @Test
    public void testSerializedToJsonAndBack() throws Exception {
        Headers headers = new RecordHeaders(
                Arrays.asList(new RecordHeader("1", "1".getBytes()), new RecordHeader("2", "2".getBytes())));

        ConsumerRecord<String, String> origin = new ConsumerRecord<>(
                "topic",
                0,
                0,
                new Date().getTime(),
                TimestampType.NO_TIMESTAMP_TYPE,
                new Date().getTime(),
                1,
                1,
                "1",
                "2",
                headers);

        ObjectMapper mapper = TestObjectMapperSingleton.INSTANCE;

        String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(origin);
        Assert.assertNotNull(json);

        ConsumerRecord<String, String> deserialized = mapper.readValue(
                json,
                new TypeReference<ConsumerRecord<String, String>>(){});
        Assert.assertNotNull(deserialized);
        Assert.assertEquals(origin.topic(), deserialized.topic());
        Assert.assertEquals(origin.partition(), deserialized.partition());
        Assert.assertEquals(origin.timestampType(), deserialized.timestampType());
        Assert.assertEquals(origin.timestamp(), deserialized.timestamp());
        Assert.assertEquals(origin.serializedKeySize(), deserialized.serializedKeySize());
        Assert.assertEquals(origin.serializedValueSize(), deserialized.serializedValueSize());
        Assert.assertEquals(origin.offset(), deserialized.offset());
        Assert.assertEquals(origin.headers(), deserialized.headers());
        Assert.assertEquals(origin.checksum(), deserialized.checksum());
        Assert.assertEquals(origin.key(), deserialized.key());
        Assert.assertEquals(origin.value(), deserialized.value());
    }

}
