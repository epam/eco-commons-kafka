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
package com.epam.eco.commons.kafka.config;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.commons.lang3.Validate;

/**
 * @author Andrei_Tytsik
 */
public abstract class AbstractConfigBuilder<T extends AbstractConfigBuilder<T>> {

    private final Map<String, Object> properties;

    protected AbstractConfigBuilder(Map<String, Object> properties) {
        this.properties = properties != null ? new HashMap<>(properties) : new HashMap<>();
    }

    @SuppressWarnings("unchecked")
    public final T properties(Map<String, Object> properties) {
        if (properties != null) {
            properties.forEach(this::property);
        }
        return (T)this;
    }

    @SuppressWarnings("unchecked")
    public final T property(String key, Object value) {
        Validate.notBlank(key, "Config key is blank");

        if (value != null) {
            properties.put(key, value);
        }
        return (T)this;
    }

    @SuppressWarnings("unchecked")
    public final T propertyIfAbsent(String key, Object value) {
        Validate.notBlank(key, "Config key is blank");

        if (value != null && properties.get(key) == null) {
            properties.put(key, value);
        }
        return (T)this;
    }

    @SuppressWarnings("unchecked")
    public final T propertyIfAbsent(String key, Supplier<?> supplier) {
        Validate.notBlank(key, "Config key is blank");

        if (properties.get(key) == null) {
            properties.put(key, supplier.get());
        }
        return (T)this;
    }

    public final Map<String, Object> build() {
        return properties;
    }

    public final Map<String, String> buildStringified() {
        return build().entrySet().stream().
                collect(
                        Collectors.toMap(
                                e -> e.getKey(),
                                e -> e.getValue().toString()));
    }

}
