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

import org.apache.kafka.common.security.auth.KafkaPrincipal;
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
public class KafkaPrincipalJsonSerializerTest {

    private static ObjectMapper objectMapper;

    @BeforeClass
    public static void setUp() {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new ParameterNamesModule())
                .registerModule(new Jdk8Module())
                .registerModule(new JavaTimeModule())
                .registerModule(new SimpleModule()
                        .addSerializer(KafkaPrincipal.class, new KafkaPrincipalJsonSerializer()));
    }

    @Test
    public void testSerialization() throws Exception {
        KafkaPrincipal origin = new KafkaPrincipal("User", "Raman_Babich@epam.com");

        String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(origin);
        Assert.assertNotNull(json);

        JsonNode jsonNode = objectMapper.readTree(json);
        Assert.assertEquals(2, jsonNode.size());
        Assert.assertEquals(origin.getPrincipalType(), jsonNode.get(KafkaPrincipalFields.PRINCIPAL_TYPE).textValue());
        Assert.assertEquals(origin.getName(), jsonNode.get(KafkaPrincipalFields.NAME).textValue());
    }

}
