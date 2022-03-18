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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

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

    @BeforeClass
    public static void setUp() {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new ParameterNamesModule())
                .registerModule(new Jdk8Module())
                .registerModule(new JavaTimeModule())
                .registerModule(new SimpleModule()
                        .addSerializer(new ConsumerRecordJsonSerializer())
                        .addSerializer(new RecordHeaderJsonSerializer())
                        .addDeserializer(Headers.class, new RecordHeadersJsonDeserializer())
                        .addDeserializer(Header.class, new RecordHeaderJsonDeserializer())
                        .addDeserializer(ConsumerRecord.class, new ConsumerRecordJsonDeserializer()));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testSerialization1() throws Exception {
        long now = new Date().getTime();
        Headers headers = new RecordHeaders(Collections.singletonList(new RecordHeader("1", "1".getBytes())));
        ConsumerRecord<String, String> origin = new ConsumerRecord<>(
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

        String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(origin);
        Assert.assertNotNull(json);

        JsonNode jsonNode = objectMapper.readTree(json);
        Assert.assertEquals(13, jsonNode.size());
        Assert.assertEquals(origin.topic(), jsonNode.get(ConsumerRecordFields.TOPIC).textValue());
        Assert.assertEquals(origin.partition(), jsonNode.get(ConsumerRecordFields.PARTITION).intValue());
        Assert.assertEquals(origin.offset(), jsonNode.get(ConsumerRecordFields.OFFSET).longValue());
        Assert.assertEquals(origin.timestamp(), jsonNode.get(ConsumerRecordFields.TIMESTAMP).longValue());
        Assert.assertEquals(origin.timestampType().name(), jsonNode.get(ConsumerRecordFields.TIMESTAMP_TYPE).textValue());
        Assert.assertEquals(origin.checksum(), jsonNode.get(ConsumerRecordFields.CHECKSUM).longValue());
        Assert.assertEquals(origin.serializedKeySize(), jsonNode.get(ConsumerRecordFields.SERIALIZED_KEY_SIZE).intValue());
        Assert.assertEquals(origin.serializedValueSize(), jsonNode.get(ConsumerRecordFields.SERIALIZED_VALUE_SIZE).intValue());
        Assert.assertEquals(origin.key().getClass().getName(), jsonNode.get(ConsumerRecordFields.KEY_CLASS).textValue());
        Assert.assertEquals(origin.key(), jsonNode.get(ConsumerRecordFields.KEY).textValue());
        Assert.assertEquals(origin.value().getClass().getName(), jsonNode.get(ConsumerRecordFields.VALUE_CLASS).textValue());
        Assert.assertEquals(origin.value(), jsonNode.get(ConsumerRecordFields.VALUE).textValue());
        Iterator<JsonNode> headerNodes = jsonNode.get(ConsumerRecordFields.HEADERS).elements();
        JsonNode tempNode = headerNodes.next();
        Header header = origin.headers().iterator().next();
        Assert.assertEquals(header.key(), tempNode.get(RecordHeaderFields.KEY).textValue());
        Assert.assertArrayEquals(header.value(), tempNode.get(RecordHeaderFields.VALUE).binaryValue());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testSerialization2() throws Exception {
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
                1L,
                1,
                1,
                youngMan,
                simpleEntity,
                headers);
        EntityWithConsumerRecord origin = new EntityWithConsumerRecord();
        origin.id = 42;
        origin.name = "name";
        origin.record = record;

        String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(origin);
        Assert.assertNotNull(json);

        JsonNode jsonNode = objectMapper.readTree(json);
        JsonNode recordNode = jsonNode.get("record");
        Assert.assertEquals(3, jsonNode.size());
        Assert.assertEquals(origin.id, jsonNode.get("id").intValue());
        Assert.assertEquals(origin.name, jsonNode.get("name").textValue());

        Assert.assertEquals(13, recordNode.size());
        Assert.assertEquals(record.topic(), recordNode.get(ConsumerRecordFields.TOPIC).textValue());
        Assert.assertEquals(record.partition(), recordNode.get(ConsumerRecordFields.PARTITION).intValue());
        Assert.assertEquals(record.offset(), recordNode.get(ConsumerRecordFields.OFFSET).longValue());
        Assert.assertEquals(record.timestamp(), recordNode.get(ConsumerRecordFields.TIMESTAMP).longValue());
        Assert.assertEquals(record.timestampType().name(), recordNode.get(ConsumerRecordFields.TIMESTAMP_TYPE).textValue());
        Assert.assertEquals(record.checksum(), recordNode.get(ConsumerRecordFields.CHECKSUM).longValue());
        Assert.assertEquals(record.serializedKeySize(), recordNode.get(ConsumerRecordFields.SERIALIZED_KEY_SIZE).intValue());
        Assert.assertEquals(record.serializedValueSize(), recordNode.get(ConsumerRecordFields.SERIALIZED_VALUE_SIZE).intValue());
        Assert.assertEquals(
                TypeFactory.defaultInstance().constructMapType(Map.class, String.class, String.class).toCanonical(),
                recordNode.get(ConsumerRecordFields.KEY_CLASS).textValue());
        Assert.assertEquals(record.key().get("Y"), recordNode.get(ConsumerRecordFields.KEY).get("Y").textValue());
        Assert.assertEquals(record.key().get("C"), recordNode.get(ConsumerRecordFields.KEY).get("C").textValue());
        Assert.assertEquals(
                SimpleType.constructUnsafe(SimpleEntity.class).toCanonical(),
                recordNode.get(ConsumerRecordFields.VALUE_CLASS).textValue());
        Assert.assertEquals(record.value().id, recordNode.get(ConsumerRecordFields.VALUE).get("id").intValue());
        Assert.assertEquals(record.value().name, recordNode.get(ConsumerRecordFields.VALUE).get("name").textValue());
        Iterator<JsonNode> headerNodes = recordNode.get(ConsumerRecordFields.HEADERS).elements();
        JsonNode tempNode = headerNodes.next();
        Header header = origin.record.headers().iterator().next();
        Assert.assertEquals(header.key(), tempNode.get(RecordHeaderFields.KEY).textValue());
        Assert.assertArrayEquals(header.value(), tempNode.get(RecordHeaderFields.VALUE).binaryValue());

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
        Assert.assertNotNull(json);

        EntityWithConsumerRecords entity = objectMapper.readValue(json, EntityWithConsumerRecords.class);
        Assert.assertEquals("3", entity.records.get(0).get(0).value().get(0).value());
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
