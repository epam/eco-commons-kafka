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
import java.util.Iterator;

import org.apache.kafka.clients.consumer.ConsumerRecord;
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
                        .addSerializer(new RecordHeaderJsonSerializer()));
    }

    @Test
    public void testSerialization() throws Exception {
        long now = new Date().getTime();
        Headers headers = new RecordHeaders(
                Arrays.asList(new RecordHeader("1", "1".getBytes()), new RecordHeader("2", "2".getBytes())));
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
        Assert.assertEquals("topic", jsonNode.get(ConsumerRecordFields.TOPIC).textValue());
        Assert.assertEquals(0, jsonNode.get(ConsumerRecordFields.PARTITION).intValue());
        Assert.assertEquals(0, jsonNode.get(ConsumerRecordFields.OFFSET).longValue());
        Assert.assertEquals(now, jsonNode.get(ConsumerRecordFields.TIMESTAMP).longValue());
        Assert.assertEquals(TimestampType.NO_TIMESTAMP_TYPE.name(), jsonNode.get(ConsumerRecordFields.TIMESTAMP_TYPE).textValue());
        Assert.assertEquals(1L, jsonNode.get(ConsumerRecordFields.CHECKSUM).longValue());
        Assert.assertEquals(1, jsonNode.get(ConsumerRecordFields.SERIALIZED_KEY_SIZE).intValue());
        Assert.assertEquals(1, jsonNode.get(ConsumerRecordFields.SERIALIZED_VALUE_SIZE).intValue());
        Assert.assertEquals(String.class.getName(), jsonNode.get(ConsumerRecordFields.KEY_CLASS).textValue());
        Assert.assertEquals("1", jsonNode.get(ConsumerRecordFields.KEY).textValue());
        Assert.assertEquals(String.class.getName(), jsonNode.get(ConsumerRecordFields.VALUE_CLASS).textValue());
        Assert.assertEquals("2", jsonNode.get(ConsumerRecordFields.VALUE).textValue());
        Iterator<JsonNode> headerNodes = jsonNode.get(ConsumerRecordFields.HEADERS).elements();
        JsonNode tempNode = headerNodes.next();
        Assert.assertEquals("1", tempNode.get(RecordHeaderFields.KEY).textValue());
        Assert.assertArrayEquals("1".getBytes(), tempNode.get(RecordHeaderFields.VALUE).binaryValue());
        tempNode = headerNodes.next();
        Assert.assertEquals("2", tempNode.get(RecordHeaderFields.KEY).textValue());
        Assert.assertArrayEquals("2".getBytes(), tempNode.get(RecordHeaderFields.VALUE).binaryValue());
        Assert.assertFalse(headerNodes.hasNext());
    }
}
