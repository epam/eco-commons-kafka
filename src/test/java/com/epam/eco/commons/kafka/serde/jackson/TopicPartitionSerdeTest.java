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

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.epam.eco.commons.kafka.util.TestObjectMapperSingleton;

/**
 * @author Raman_Babich
 */
public class TopicPartitionSerdeTest {

    @Test
    public void testSerializedToJsonAndBack() throws Exception {
        TopicPartition origin = new TopicPartition("topic", 1);

        ObjectMapper mapper = TestObjectMapperSingleton.INSTANCE;

        String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(origin);
        Assert.assertNotNull(json);

        TopicPartition deserialized = mapper.readValue(
                json,
                TopicPartition.class);
        Assert.assertNotNull(deserialized);
        Assert.assertEquals(deserialized, origin);
    }

    @Test
    public void testKeySerializedToJsonAndBack() throws Exception {
        Map<TopicPartition, String> origin = new HashMap<>();
        origin.put(new TopicPartition("topic", 0), "0");
        origin.put(new TopicPartition("topic", 1), "1");
        origin.put(new TopicPartition("topic", 2), "2");

        ObjectMapper mapper = TestObjectMapperSingleton.INSTANCE;

        String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(origin);
        Assert.assertNotNull(json);

        Map<TopicPartition, String> deserialized = mapper.readValue(
                json,
                new TypeReference<Map<TopicPartition, String>>(){});
        Assert.assertNotNull(deserialized);
        Assert.assertEquals(deserialized, origin);
    }

}
