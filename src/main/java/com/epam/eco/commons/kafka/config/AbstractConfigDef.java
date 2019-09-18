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
package com.epam.eco.commons.kafka.config;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.commons.lang3.Validate;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.ConfigKey;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

/**
 * @author Andrei_Tytsik
 */
public abstract class AbstractConfigDef {

    private final ConfigDef def;

    public AbstractConfigDef(ConfigDef def) {
        Validate.notNull(def, "Config def is null");

        this.def = def;
    }

    public List<ConfigKey> keys() {
        return def.configKeys().values().stream().
                sorted(ConfigKeyComparator.INSTANCE).
                collect(Collectors.toList());
    }

    public ConfigKey key(String name) {
        return def.configKeys().get(name);
    }

    public String doc(String name) {
        ConfigKey key = key(name);
        return key != null ? key.documentation : null;
    }

    public Importance importance(String name) {
        ConfigKey key = key(name);
        return key != null ? key.importance : null;
    }

    public Type type(String name) {
        ConfigKey key = key(name);
        return key != null ? key.type : null;
    }

    public boolean isDefaultValue(String name, Object value) {
        Object defaultValue = defaultValue(name);
        if (Objects.equals(defaultValue, value)) {
            return true;
        }

        defaultValue = defaultValue != null ? defaultValue.toString() : null;
        value = value != null ? value.toString() : null;

        return Objects.equals(defaultValue, value);
    }

    public Object defaultValue(String name) {
        ConfigKey key = key(name);
        return key != null ? key.defaultValue : null;
    }

}
