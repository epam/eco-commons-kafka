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

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;

/**
 * @author Raman_Babich
 */
public class RecordHeaderJsonDeserializerTest {

    private static ObjectMapper objectMapper;

    @BeforeAll
    public static void setUp() {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new ParameterNamesModule())
                .registerModule(new Jdk8Module())
                .registerModule(new JavaTimeModule())
                .registerModule(new SimpleModule()
                        .addDeserializer(RecordHeader.class, new RecordHeaderJsonDeserializer())
                        .addDeserializer(Header.class, new RecordHeaderJsonDeserializer(Header.class)));
    }

    @Test
    public void testDeserialization() throws Exception {
        Header expected = new RecordHeader("1", "1".getBytes());

        ObjectNode objectNode = objectMapper.createObjectNode();
        objectNode.put(RecordHeaderFields.KEY, expected.key());
        objectNode.put(RecordHeaderFields.VALUE, expected.value());

        String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(objectNode);
        Assertions.assertNotNull(json);

        Header actual = objectMapper.readValue(json, Header.class);
        Assertions.assertNotNull(actual);
        Assertions.assertEquals(expected, actual);
    }
}
