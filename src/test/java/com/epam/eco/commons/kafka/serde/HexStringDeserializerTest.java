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
package com.epam.eco.commons.kafka.serde;

import java.math.BigInteger;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author Andrei_Tytsik
 */
public class HexStringDeserializerTest {

    private final HexStringDeserializer deserializer = new HexStringDeserializer();

    @Test
    public void testHexStringIsDeserialized() {
        byte[] bytes = BigInteger.valueOf(65535).toByteArray();

        String hex = deserializer.deserialize(null, bytes);

        Assertions.assertEquals("00ffff", hex);
    }

    @Test
    public void testNullInputGivesNullOutput() {
        Assertions.assertNull(deserializer.deserialize(null, null));
    }

}
