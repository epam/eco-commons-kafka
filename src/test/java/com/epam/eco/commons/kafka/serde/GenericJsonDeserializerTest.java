/*
 * Copyright 2020 EPAM Systems
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
package com.epam.eco.commons.kafka.serde;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Andrei_Tytsik
 */
public class GenericJsonDeserializerTest {

    private GenericJsonDeserializer deserializer = new GenericJsonDeserializer();
    {
        deserializer.configure(Collections.emptyMap(), true);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testGenericJsonIsDeserialized() throws Exception {
        String jsonOrig = "{\"a\":\"a\",\"b\":10,\"c\":{\"d\":1.0}}";

        byte[] bytes = jsonOrig.getBytes(StandardCharsets.UTF_8);

        Map<String, Object> json = deserializer.deserialize(null, bytes);

        Assert.assertNotNull(json);

        Assert.assertTrue(json.containsKey("a"));
        Assert.assertEquals("a", json.get("a"));

        Assert.assertTrue(json.containsKey("b"));
        Assert.assertEquals(10, json.get("b"));

        Assert.assertTrue(json.containsKey("c"));
        Object jsonC = json.get("c");
        Assert.assertNotNull(jsonC);
        Assert.assertTrue(jsonC instanceof Map);
        Assert.assertTrue(((Map<String, Object>)jsonC).containsKey("d"));
        Assert.assertEquals(1.0, ((Map<String, Object>)jsonC).get("d"));
    }

    @Test
    public void testNullInputGivesNullOutput() throws Exception {
        Assert.assertNull(deserializer.deserialize(null, null));
    }

}
