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

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;

import com.epam.eco.commons.kafka.serde.jackson.HeaderJsonDeserializer;
import com.epam.eco.commons.kafka.serde.jackson.HeadersJsonDeserializer;

/**
 * @author Raman_Babich
 */
public class HeadersJsonDeserializerTest {

    private ObjectMapper objectMapper;

    @Before
    public void setUp() {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new ParameterNamesModule())
                .registerModule(new Jdk8Module())
                .registerModule(new JavaTimeModule())
                .registerModule(new SimpleModule()
                        .addDeserializer(Headers.class, new HeadersJsonDeserializer())
                        .addDeserializer(Header.class, new HeaderJsonDeserializer()));
    }

    @Test
    public void testDeserialization() throws Exception {
        Headers expected = new RecordHeaders(
                Arrays.asList(new RecordHeader("1", "1".getBytes()), new RecordHeader("2", "2".getBytes())));

        ArrayNode arrayNode = objectMapper.createArrayNode();
        arrayNode.addObject()
                .put("key", "1")
                .put("value", "1".getBytes());
        arrayNode.addObject()
                .put("key", "2")
                .put("value", "2".getBytes());

        String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(arrayNode);
        Assert.assertNotNull(json);

        Headers actual = objectMapper.readValue(json, Headers.class);
        Assert.assertNotNull(actual);
        Assert.assertEquals(expected, actual);
    }
}
