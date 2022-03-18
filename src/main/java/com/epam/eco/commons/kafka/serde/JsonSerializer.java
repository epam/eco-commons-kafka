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

import java.io.IOException;
import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

/**
 * @author Andrei_Tytsik
 */
public class JsonSerializer implements Serializer<Object> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, Object data) {
        if (data == null) {
            return null;
        }

        try {
            return ObjectMapperSingleton.INSTANCE.writeValueAsBytes(data);
        } catch (IOException ioe) {
            throw new RuntimeException(
                    String.format("Failed to serialize JSON object %s", data), ioe);
        }
    }

    @Override
    public void close() {
    }

}
