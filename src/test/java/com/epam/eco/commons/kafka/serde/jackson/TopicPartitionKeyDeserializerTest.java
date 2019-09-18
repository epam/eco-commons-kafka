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

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;

import com.epam.eco.commons.kafka.serde.jackson.TopicPartitionKeyDeserializer;

/**
 * @author Raman_Babich
 */
public class TopicPartitionKeyDeserializerTest {

    private ObjectMapper objectMapper;

    @Before
    public void setUp() throws Exception {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new ParameterNamesModule())
                .registerModule(new Jdk8Module())
                .registerModule(new JavaTimeModule())
                .registerModule(new SimpleModule()
                        .addKeyDeserializer(TopicPartition.class, new TopicPartitionKeyDeserializer()));
    }

    @Test
    public void testKeyDeserialization() throws Exception {
        Map<TopicPartition, String> expected = new HashMap<>();
        expected.put(new TopicPartition("topic", 0), "topic-0");
        expected.put(new TopicPartition("topic", 1), "topic-1");
        expected.put(new TopicPartition("topic", 2), "topic-2");

        ObjectNode objectNode = objectMapper.createObjectNode();
        objectNode.put("topic-0", "topic-0");
        objectNode.put("topic-1", "topic-1");
        objectNode.put("topic-2", "topic-2");

        String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(objectNode);
        Assert.assertNotNull(json);

        Map<TopicPartition, String> actual = objectMapper.readValue(
                json,
                new TypeReference<Map<TopicPartition, String>>(){});
        Assert.assertEquals(expected, actual);
    }
}
