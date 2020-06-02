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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * @author Andrei_Tytsik
 */
public class JsonStringDeserializer implements Deserializer<String> {

    public static final String PRETTY = ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG + ".jsonstring.pretty";

    private boolean pretty;

    public void configure(Map<String, ?> configs, boolean isKey) {
        pretty = readPrettyProp(configs);
    }

    public String deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }

        try {
            Object jsonObject = ObjectMapperSingleton.INSTANCE.
                    readValue(new String(data, StandardCharsets.UTF_8), Object.class);
            if (pretty) {
                return ObjectMapperSingleton.INSTANCE.
                        writerWithDefaultPrettyPrinter().writeValueAsString(jsonObject);
            }
            return ObjectMapperSingleton.INSTANCE.writeValueAsString(jsonObject);
        } catch (IOException ioe) {
            throw new RuntimeException("Failed to deserialize JSON string", ioe);
        }
    }

    public void close() {
    }

    private boolean readPrettyProp(Map<String, ?> configs) {
        Object prettyObj = configs.get(PRETTY);
        if (prettyObj == null) {
            return false;
        }

        if (prettyObj instanceof Boolean) {
            return (Boolean)prettyObj;
        } else  if (prettyObj instanceof String) {
            return Boolean.parseBoolean((String)prettyObj);
        } else {
            throw new IllegalArgumentException(
                    String.format("Illegal value for configuration '%s': %s", PRETTY, prettyObj));
        }
    }

}
