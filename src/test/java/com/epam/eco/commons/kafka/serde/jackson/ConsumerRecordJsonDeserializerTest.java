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
package com.epam.eco.commons.kafka.serde.jackson;

import java.util.Arrays;
import java.util.Date;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;

import com.epam.eco.commons.kafka.serde.jackson.ConsumerRecordJsonDeserializer;
import com.epam.eco.commons.kafka.serde.jackson.HeaderJsonDeserializer;
import com.epam.eco.commons.kafka.serde.jackson.HeadersJsonDeserializer;

/**
 * @author Raman_Babich
 */
public class ConsumerRecordJsonDeserializerTest {

    private ObjectMapper objectMapper;

    @Before
    public void setUp() {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new ParameterNamesModule())
                .registerModule(new Jdk8Module())
                .registerModule(new JavaTimeModule())
                .registerModule(new SimpleModule()
                        .addDeserializer(Headers.class, new HeadersJsonDeserializer())
                        .addDeserializer(Header.class, new HeaderJsonDeserializer())
                        .addDeserializer(ConsumerRecord.class, new ConsumerRecordJsonDeserializer()));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testDeserialization() throws Exception {
        long now = new Date().getTime();
        Headers headers = new RecordHeaders(
                Arrays.asList(new RecordHeader("1", "1".getBytes()), new RecordHeader("2", "2".getBytes())));
        ConsumerRecord<String, String> expected = new ConsumerRecord<>(
                "topic",
                0,
                0,
                now,
                TimestampType.NO_TIMESTAMP_TYPE,
                1L,
                1,
                1,
                "1",
                "2",
                headers);

        ObjectNode objectNode = objectMapper.createObjectNode();
        objectNode.put("topic", "topic");
        objectNode.put("partition", 0);
        objectNode.put("offset", 0);
        objectNode.put("timestamp", now);
        objectNode.put("timestampType", "NoTimestampType");
        objectNode.put("checksum", 1L);
        objectNode.put("serializedKeySize", 1);
        objectNode.put("serializedValueSize", 1);
        objectNode.put("@keyClass", String.class.getName());
        objectNode.put("key", "1");
        objectNode.put("@valueClass", String.class.getName());
        objectNode.put("value", "2");
        ArrayNode headerArray = objectNode.putArray("headers");
        headerArray.addObject()
                .put("key", "1")
                .put("value", "1".getBytes());
        headerArray.addObject()
                .put("key", "2")
                .put("value", "2".getBytes());

        String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(objectNode);
        Assert.assertNotNull(json);

        ConsumerRecord<String, String> actual = objectMapper.readValue(
                json,
                new TypeReference<ConsumerRecord<String, String>>() {});
        Assert.assertNotNull(actual);
        Assert.assertEquals(expected.topic(), actual.topic());
        Assert.assertEquals(expected.partition(), actual.partition());
        Assert.assertEquals(expected.timestampType(), actual.timestampType());
        Assert.assertEquals(expected.timestamp(), actual.timestamp());
        Assert.assertEquals(expected.serializedKeySize(), actual.serializedKeySize());
        Assert.assertEquals(expected.serializedValueSize(), actual.serializedValueSize());
        Assert.assertEquals(expected.offset(), actual.offset());
        Assert.assertEquals(expected.headers(), actual.headers());
        Assert.assertEquals(expected.checksum(), actual.checksum());
        Assert.assertEquals(expected.key(), actual.key());
        Assert.assertEquals(expected.value(), actual.value());
    }
}
