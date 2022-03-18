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

import java.util.Collections;
import java.util.Date;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;

/**
 * @author Raman_Babich
 */
public class ConsumerRecordJsonDeserializerTest {

    private static ObjectMapper objectMapper;

    @BeforeClass
    public static void setUp() {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new ParameterNamesModule())
                .registerModule(new Jdk8Module())
                .registerModule(new JavaTimeModule())
                .registerModule(new SimpleModule()
                        .addDeserializer(Headers.class, new RecordHeadersJsonDeserializer())
                        .addDeserializer(Header.class, new RecordHeaderJsonDeserializer())
                        .addDeserializer(ConsumerRecord.class, new ConsumerRecordJsonDeserializer()));
    }

    @SuppressWarnings({ "deprecation", "rawtypes" })
    private static boolean consumerRecordEquals(ConsumerRecord a, ConsumerRecord b) {
        return
                StringUtils.equals(a.topic(), b.topic()) &&
                        Objects.equals(a.partition(), b.partition()) &&
                        Objects.equals(a.offset(), b.offset()) &&
                        Objects.equals(a.timestamp(), b.timestamp()) &&
                        Objects.equals(a.timestampType(), b.timestampType()) &&
                        Objects.equals(a.checksum(), b.checksum()) &&
                        Objects.equals(a.serializedKeySize(), b.serializedKeySize()) &&
                        Objects.equals(a.serializedValueSize(), b.serializedValueSize()) &&
                        Objects.equals(a.key(), b.key()) &&
                        Objects.equals(a.value(), b.value()) &&
                        Objects.equals(a.headers(), b.headers());

    }

    @SuppressWarnings("deprecation")
    @Test
    public void testDeserialization1() throws Exception {
        long now = new Date().getTime();
        Headers headers = new RecordHeaders(
                Collections.singletonList(new RecordHeader("1", "1".getBytes())));
        ConsumerRecord<String, String> expected = new ConsumerRecord<>(
                "topic",
                1,
                1,
                now,
                TimestampType.NO_TIMESTAMP_TYPE,
                1L,
                1,
                1,
                "1",
                "2",
                headers);

        ObjectNode objectNode = objectMapper.createObjectNode();
        objectNode.put(ConsumerRecordFields.TOPIC, expected.topic());
        objectNode.put(ConsumerRecordFields.PARTITION, expected.partition());
        objectNode.put(ConsumerRecordFields.OFFSET, expected.offset());
        objectNode.put(ConsumerRecordFields.TIMESTAMP, expected.timestamp());
        objectNode.put(ConsumerRecordFields.TIMESTAMP_TYPE, expected.timestampType().name());
        objectNode.put(ConsumerRecordFields.CHECKSUM, expected.checksum());
        objectNode.put(ConsumerRecordFields.SERIALIZED_KEY_SIZE, expected.serializedKeySize());
        objectNode.put(ConsumerRecordFields.SERIALIZED_VALUE_SIZE, expected.serializedValueSize());
        objectNode.put(ConsumerRecordFields.KEY_CLASS, String.class.getName());
        objectNode.put(ConsumerRecordFields.KEY, expected.key());
        objectNode.put(ConsumerRecordFields.VALUE_CLASS, String.class.getName());
        objectNode.put(ConsumerRecordFields.VALUE, expected.value());
        ArrayNode headerArray = objectNode.putArray(ConsumerRecordFields.HEADERS);
        Header header = headers.iterator().next();
        headerArray.addObject()
                .put(RecordHeaderFields.KEY, header.key())
                .put(RecordHeaderFields.VALUE, header.value());

        String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(objectNode);
        Assert.assertNotNull(json);

        ConsumerRecord<String, String> actual = objectMapper.readValue(
                json,
                new TypeReference<ConsumerRecord<String, String>>() {});
        Assert.assertNotNull(actual);
        Assert.assertTrue(consumerRecordEquals(expected, actual));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testDeserialization2() throws Exception {
        long now = new Date().getTime();
        Headers headers = new RecordHeaders(
                Collections.singletonList(new RecordHeader("1", "1".getBytes())));
        ConsumerRecord<String, String> expected = new ConsumerRecord<>(
                "topic",
                1,
                1,
                now,
                TimestampType.NO_TIMESTAMP_TYPE,
                1L,
                1,
                1,
                "1",
                "2",
                headers);

        ObjectNode objectNode = objectMapper.createObjectNode();
        objectNode.put(ConsumerRecordFields.TOPIC, expected.topic());
        objectNode.put(ConsumerRecordFields.PARTITION, expected.partition());
        objectNode.put(ConsumerRecordFields.OFFSET, expected.offset());
        objectNode.put(ConsumerRecordFields.TIMESTAMP, expected.timestamp());
        objectNode.put(ConsumerRecordFields.TIMESTAMP_TYPE, expected.timestampType().name());
        objectNode.put(ConsumerRecordFields.CHECKSUM, expected.checksum());
        objectNode.put(ConsumerRecordFields.SERIALIZED_KEY_SIZE, expected.serializedKeySize());
        objectNode.put(ConsumerRecordFields.SERIALIZED_VALUE_SIZE, expected.serializedValueSize());
        objectNode.put(ConsumerRecordFields.KEY, expected.key());
        objectNode.put(ConsumerRecordFields.VALUE, expected.value());
        ArrayNode headerArray = objectNode.putArray(ConsumerRecordFields.HEADERS);
        Header header = headers.iterator().next();
        headerArray.addObject()
                .put(RecordHeaderFields.KEY, header.key())
                .put(RecordHeaderFields.VALUE, header.value());

        String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(objectNode);
        Assert.assertNotNull(json);

        ConsumerRecord<String, String> actual = objectMapper.readValue(
                json,
                new TypeReference<ConsumerRecord<String, String>>() {});
        Assert.assertNotNull(actual);
        Assert.assertTrue(consumerRecordEquals(expected, actual));
    }

    @SuppressWarnings({ "deprecation", "rawtypes" })
    @Test
    public void testDeserialization3() throws Exception {
        long now = new Date().getTime();
        Headers headers = new RecordHeaders(
                Collections.singletonList(new RecordHeader("1", "1".getBytes())));
        ConsumerRecord<String, String> expected = new ConsumerRecord<>(
                "topic",
                1,
                1,
                now,
                TimestampType.NO_TIMESTAMP_TYPE,
                1L,
                1,
                1,
                "1",
                "2",
                headers);

        ObjectNode objectNode = objectMapper.createObjectNode();
        objectNode.put(ConsumerRecordFields.TOPIC, expected.topic());
        objectNode.put(ConsumerRecordFields.PARTITION, expected.partition());
        objectNode.put(ConsumerRecordFields.OFFSET, expected.offset());
        objectNode.put(ConsumerRecordFields.TIMESTAMP, expected.timestamp());
        objectNode.put(ConsumerRecordFields.TIMESTAMP_TYPE, expected.timestampType().name());
        objectNode.put(ConsumerRecordFields.CHECKSUM, expected.checksum());
        objectNode.put(ConsumerRecordFields.SERIALIZED_KEY_SIZE, expected.serializedKeySize());
        objectNode.put(ConsumerRecordFields.SERIALIZED_VALUE_SIZE, expected.serializedValueSize());
        objectNode.put(ConsumerRecordFields.KEY, expected.key());
        objectNode.put(ConsumerRecordFields.VALUE, expected.value());
        ArrayNode headerArray = objectNode.putArray(ConsumerRecordFields.HEADERS);
        Header header = headers.iterator().next();
        headerArray.addObject()
                .put(RecordHeaderFields.KEY, header.key())
                .put(RecordHeaderFields.VALUE, header.value());

        String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(objectNode);
        Assert.assertNotNull(json);

        ConsumerRecord actual = objectMapper.readValue(
                json,
                ConsumerRecord.class);
        Assert.assertTrue(consumerRecordEquals(expected, actual));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testDeserialization4() throws Exception {
        long now = new Date().getTime();
        Headers headers = new RecordHeaders(
                Collections.singletonList(new RecordHeader("1", "1".getBytes())));
        ConsumerRecord<String, String> expected = new ConsumerRecord<>(
                "topic",
                1,
                1,
                now,
                TimestampType.NO_TIMESTAMP_TYPE,
                1L,
                1,
                1,
                null,
                null,
                headers);

        ObjectNode objectNode = objectMapper.createObjectNode();
        objectNode.put(ConsumerRecordFields.TOPIC, expected.topic());
        objectNode.put(ConsumerRecordFields.PARTITION, expected.partition());
        objectNode.put(ConsumerRecordFields.OFFSET, expected.offset());
        objectNode.put(ConsumerRecordFields.TIMESTAMP, expected.timestamp());
        objectNode.put(ConsumerRecordFields.TIMESTAMP_TYPE, expected.timestampType().name());
        objectNode.put(ConsumerRecordFields.CHECKSUM, expected.checksum());
        objectNode.put(ConsumerRecordFields.SERIALIZED_KEY_SIZE, expected.serializedKeySize());
        objectNode.put(ConsumerRecordFields.SERIALIZED_VALUE_SIZE, expected.serializedValueSize());
        objectNode.put(ConsumerRecordFields.KEY, expected.key());
        objectNode.put(ConsumerRecordFields.VALUE, expected.value());
        ArrayNode headerArray = objectNode.putArray(ConsumerRecordFields.HEADERS);
        Header header = headers.iterator().next();
        headerArray.addObject()
                .put(RecordHeaderFields.KEY, header.key())
                .put(RecordHeaderFields.VALUE, header.value());

        String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(objectNode);
        Assert.assertNotNull(json);

        ConsumerRecord<String, String> actual = objectMapper.readValue(
                json,
                new TypeReference<ConsumerRecord<String, String>>() {});
        Assert.assertTrue(consumerRecordEquals(expected, actual));
    }
}
