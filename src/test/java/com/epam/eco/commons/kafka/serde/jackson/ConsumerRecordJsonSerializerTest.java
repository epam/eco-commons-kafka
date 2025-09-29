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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.type.SimpleType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;

/**
 * @author Raman_Babich
 */
public class ConsumerRecordJsonSerializerTest {

    private static ObjectMapper objectMapper;

    @BeforeAll
    public static void setUp() {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new ParameterNamesModule())
                .registerModule(new Jdk8Module())
                .registerModule(new JavaTimeModule())
                .registerModule(new SimpleModule()
                        .addSerializer(new ConsumerRecordJsonSerializer())
                        .addSerializer(new RecordHeaderJsonSerializer())
                        .addSerializer(new RecordHeadersJsonSerializer())
                        .addDeserializer(Headers.class, new RecordHeadersJsonDeserializer())
                        .addDeserializer(Header.class, new RecordHeaderJsonDeserializer())
                        .addDeserializer(ConsumerRecord.class, new ConsumerRecordJsonDeserializer()));
    }

    @Test
    public void testSerialization1() throws IOException {
        long now = new Date().getTime();
        Headers headers = new RecordHeaders(Collections.singletonList(new RecordHeader("1", "1".getBytes())));
        ConsumerRecord<String, String> origin = new ConsumerRecord<>(
                "topic",
                0,
                0,
                now,
                TimestampType.NO_TIMESTAMP_TYPE,
                1,
                1,
                "1",
                "2",
                headers,
                Optional.empty());

        String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(origin);
        Assertions.assertNotNull(json);

        JsonNode jsonNode = objectMapper.readTree(json);
        Assertions.assertEquals(12, jsonNode.size());
        Assertions.assertEquals(origin.topic(), jsonNode.get(ConsumerRecordFields.TOPIC).textValue());
        Assertions.assertEquals(origin.partition(), jsonNode.get(ConsumerRecordFields.PARTITION).intValue());
        Assertions.assertEquals(origin.offset(), jsonNode.get(ConsumerRecordFields.OFFSET).longValue());
        Assertions.assertEquals(origin.timestamp(), jsonNode.get(ConsumerRecordFields.TIMESTAMP).longValue());
        Assertions.assertEquals(origin.timestampType().name(), jsonNode.get(ConsumerRecordFields.TIMESTAMP_TYPE).textValue());
        Assertions.assertEquals(origin.serializedKeySize(), jsonNode.get(ConsumerRecordFields.SERIALIZED_KEY_SIZE).intValue());
        Assertions.assertEquals(origin.serializedValueSize(), jsonNode.get(ConsumerRecordFields.SERIALIZED_VALUE_SIZE).intValue());
        Assertions.assertEquals(origin.key().getClass().getName(), jsonNode.get(ConsumerRecordFields.KEY_CLASS).textValue());
        Assertions.assertEquals(origin.key(), jsonNode.get(ConsumerRecordFields.KEY).textValue());
        Assertions.assertEquals(origin.value().getClass().getName(), jsonNode.get(ConsumerRecordFields.VALUE_CLASS).textValue());
        Assertions.assertEquals(origin.value(), jsonNode.get(ConsumerRecordFields.VALUE).textValue());
        Iterator<JsonNode> headerNodes = jsonNode.get(ConsumerRecordFields.HEADERS).elements();
        JsonNode tempNode = headerNodes.next();
        Header header = origin.headers().iterator().next();
        Assertions.assertEquals(header.key(), tempNode.get(RecordHeaderFields.KEY).textValue());
        Assertions.assertArrayEquals(header.value(), tempNode.get(RecordHeaderFields.VALUE).binaryValue());
    }

    @Test
    public void testSerialization2() throws IOException {
        SimpleEntity simpleEntity = new SimpleEntity();
        simpleEntity.id = 420;
        simpleEntity.name = "It is time!";
        Map<String, String> youngMan = new HashMap<>();
        youngMan.put("Y", "M");
        youngMan.put("C", "A");
        long now = new Date().getTime();
        Headers headers = new RecordHeaders(Collections.singletonList(new RecordHeader("1", "1".getBytes())));
        ConsumerRecord<Map<String, String>, SimpleEntity> record = new ConsumerRecord<>(
                "topic",
                0,
                0,
                now,
                TimestampType.NO_TIMESTAMP_TYPE,
                1,
                1,
                youngMan,
                simpleEntity,
                headers,
                Optional.empty());

        EntityWithConsumerRecord origin = new EntityWithConsumerRecord();
        origin.id = 42;
        origin.name = "name";
        origin.record = record;

        String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(origin);
        Assertions.assertNotNull(json);

        JsonNode jsonNode = objectMapper.readTree(json);
        JsonNode recordNode = jsonNode.get("record");
        Assertions.assertEquals(3, jsonNode.size());
        Assertions.assertEquals(origin.id, jsonNode.get("id").intValue());
        Assertions.assertEquals(origin.name, jsonNode.get("name").textValue());

        Assertions.assertEquals(12, recordNode.size());
        Assertions.assertEquals(record.topic(), recordNode.get(ConsumerRecordFields.TOPIC).textValue());
        Assertions.assertEquals(record.partition(), recordNode.get(ConsumerRecordFields.PARTITION).intValue());
        Assertions.assertEquals(record.offset(), recordNode.get(ConsumerRecordFields.OFFSET).longValue());
        Assertions.assertEquals(record.timestamp(), recordNode.get(ConsumerRecordFields.TIMESTAMP).longValue());
        Assertions.assertEquals(record.timestampType().name(), recordNode.get(ConsumerRecordFields.TIMESTAMP_TYPE).textValue());
        Assertions.assertEquals(record.serializedKeySize(), recordNode.get(ConsumerRecordFields.SERIALIZED_KEY_SIZE).intValue());
        Assertions.assertEquals(record.serializedValueSize(), recordNode.get(ConsumerRecordFields.SERIALIZED_VALUE_SIZE).intValue());
        Assertions.assertEquals(
                TypeFactory.defaultInstance().constructMapType(Map.class, String.class, String.class).toCanonical(),
                recordNode.get(ConsumerRecordFields.KEY_CLASS).textValue());
        Assertions.assertEquals(record.key().get("Y"), recordNode.get(ConsumerRecordFields.KEY).get("Y").textValue());
        Assertions.assertEquals(record.key().get("C"), recordNode.get(ConsumerRecordFields.KEY).get("C").textValue());
        Assertions.assertEquals(
                SimpleType.constructUnsafe(SimpleEntity.class).toCanonical(),
                recordNode.get(ConsumerRecordFields.VALUE_CLASS).textValue());
        Assertions.assertEquals(record.value().id, recordNode.get(ConsumerRecordFields.VALUE).get("id").intValue());
        Assertions.assertEquals(record.value().name, recordNode.get(ConsumerRecordFields.VALUE).get("name").textValue());
        Iterator<JsonNode> headerNodes = recordNode.get(ConsumerRecordFields.HEADERS).elements();
        JsonNode tempNode = headerNodes.next();
        Header header = origin.record.headers().iterator().next();
        Assertions.assertEquals(header.key(), tempNode.get(RecordHeaderFields.KEY).textValue());
        Assertions.assertArrayEquals(header.value(), tempNode.get(RecordHeaderFields.VALUE).binaryValue());

    }

    @Test
    public void testSerialization3() throws IOException {
        EntityWithConsumerRecords origin = new EntityWithConsumerRecords();
        origin.records = new ArrayList<>();
        origin.records.add(Collections.singletonList(
                new ConsumerRecord<>(
                        "topic",
                        0,
                        0,
                        "1",
                        Collections.singletonList(
                                new ConsumerRecord<>("topic", 0, 0, "2", "3")))));

        String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(origin);
        Assertions.assertNotNull(json);

        EntityWithConsumerRecords entity = objectMapper.readValue(json, EntityWithConsumerRecords.class);
        Assertions.assertEquals("3", entity.records.get(0).get(0).value().get(0).value());
    }

    private static class EntityWithConsumerRecords {
        public List<List<ConsumerRecord<String, List<ConsumerRecord<String, String>>>>> records;
    }

    private static class EntityWithConsumerRecord {
        public int id;
        public String name;
        public ConsumerRecord<Map<String, String>, SimpleEntity> record;
    }

    private static class SimpleEntity {
        public int id;
        public String name;
    }

}
