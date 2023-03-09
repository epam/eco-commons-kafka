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
package com.epam.eco.commons.kafka.serde.jackson;

import java.util.Arrays;
import java.util.Date;
import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.epam.eco.commons.kafka.util.TestObjectMapperSingleton;

/**
 * @author Raman_Babich
 */
public class ConsumerRecordSerdeTest {

    @Test
    public void testSerializedToJsonAndBack() throws JsonProcessingException {
        Headers headers = new RecordHeaders(
                Arrays.asList(new RecordHeader("1", "1".getBytes()), new RecordHeader("2", "2".getBytes())));

        ConsumerRecord<String, String> origin = new ConsumerRecord<>(
                "topic",
                0,
                0,
                new Date().getTime(),
                TimestampType.NO_TIMESTAMP_TYPE,
                1,
                1,
                "1",
                "2",
                headers,
                Optional.empty());

        ObjectMapper mapper = TestObjectMapperSingleton.INSTANCE;

        String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(origin);
        Assertions.assertNotNull(json);

        ConsumerRecord<String, String> deserialized = mapper.readValue(
                json,
                new TypeReference<>(){});
        Assertions.assertNotNull(deserialized);
        Assertions.assertEquals(origin.topic(), deserialized.topic());
        Assertions.assertEquals(origin.partition(), deserialized.partition());
        Assertions.assertEquals(origin.timestampType(), deserialized.timestampType());
        Assertions.assertEquals(origin.timestamp(), deserialized.timestamp());
        Assertions.assertEquals(origin.serializedKeySize(), deserialized.serializedKeySize());
        Assertions.assertEquals(origin.serializedValueSize(), deserialized.serializedValueSize());
        Assertions.assertEquals(origin.offset(), deserialized.offset());
        Assertions.assertEquals(origin.headers(), deserialized.headers());
        Assertions.assertEquals(origin.key(), deserialized.key());
        Assertions.assertEquals(origin.value(), deserialized.value());
    }

}
