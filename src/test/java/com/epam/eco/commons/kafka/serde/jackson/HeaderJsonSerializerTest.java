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
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;

import com.epam.eco.commons.kafka.serde.jackson.HeaderJsonSerializer;

/**
 * @author Raman_Babich
 */
public class HeaderJsonSerializerTest {

    private ObjectMapper objectMapper;

    @Before
    public void setUp() {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new ParameterNamesModule())
                .registerModule(new Jdk8Module())
                .registerModule(new JavaTimeModule())
                .registerModule(new SimpleModule()
                        .addSerializer(new HeaderJsonSerializer()));
    }

    @Test
    public void testSerialization() throws Exception {
        Header origin = new RecordHeader("1", "1".getBytes());

        String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(origin);
        Assert.assertNotNull(json);

        JsonNode jsonNode = objectMapper.readTree(json);
        Assert.assertEquals("1", jsonNode.get("key").textValue());
        Assert.assertTrue(Arrays.equals("1".getBytes(), jsonNode.get("value").binaryValue()));
    }

}
