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

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.epam.eco.commons.kafka.util.TestObjectMapperSingleton;

/**
 * @author Raman_Babich
 */
public class HeadersSerdeTest {

    @Test
    public void testSerializedToJsonAndBack() throws Exception {
        Headers origin = new RecordHeaders(
                Arrays.asList(
                        new RecordHeader("1", "1".getBytes()),
                        new RecordHeader("2", "2".getBytes())));

        ObjectMapper mapper = TestObjectMapperSingleton.INSTANCE;

        String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(origin);
        Assert.assertNotNull(json);

        Headers deserialized = mapper.readValue(
                json,
                Headers.class);
        Assert.assertNotNull(deserialized);
        Assert.assertEquals(deserialized, origin);
    }

}
