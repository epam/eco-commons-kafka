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
package com.epam.eco.commons.kafka.serde;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.lang3.ClassUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * @author Andrei_Tytsik
 */
public class JsonDeserializer<T> implements Deserializer<T> {

    public static final String KEY_TYPE =
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG + ".json.type";

    public static final String VALUE_TYPE =
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG + ".json.type";

    private Class<T> type;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        type = initTypeConfig(configs, isKey);
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }

        try {
            return ObjectMapperSingleton.INSTANCE.readValue(data, type);
        } catch (IOException ioe) {
            throw new RuntimeException("Failed to deserialize JSON object", ioe);
        }
    }

    @Override
    public void close() {
    }

    @SuppressWarnings("unchecked")
    private Class<T> initTypeConfig(Map<String, ?> configs, boolean isKey) {
        String typeConfigKey = isKey ? KEY_TYPE : VALUE_TYPE;

        if (configs.get(typeConfigKey) == null) {
            throw new RuntimeException(
                    String.format("Configuration '%s' is missing", typeConfigKey));
        }

        Object typeOrName = configs.get(typeConfigKey);
        if (typeOrName instanceof Class) {
            return (Class<T>)typeOrName;
        } else if (typeOrName instanceof String) {
            try {
                return (Class<T>) ClassUtils.getClass((String)typeOrName);
            } catch (ClassNotFoundException cnfe) {
                throw new RuntimeException(cnfe);
            }
        } else {
            throw new IllegalArgumentException(
                    String.format("Illegal value for configuration '%s': %s", typeConfigKey, typeOrName));
        }
    }

}
