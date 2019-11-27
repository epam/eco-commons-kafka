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

    public ConfigKey keyRequired(String name) {
        ConfigKey key = key(name);
        if (key == null) {
            throw new IllegalArgumentException("No config key found for name: " + name);
        }

        return key;
    }

    public ConfigKey key(String name) {
        return def.configKeys().get(name);
    }

    public String doc(String name) {
        return keyRequired(name).documentation;
    }

    public Importance importance(String name) {
        return keyRequired(name).importance;
    }

    public Type type(String name) {
        return keyRequired(name).type;
    }

    public boolean isDefaultValue(String name, Object value) {
        return
                Objects.equals(defaultValue(name), value) ||
                (value instanceof String && Objects.equals(defaultValueAsString(name), value));
    }

    public Object defaultValue(String name) {
        return keyRequired(name).defaultValue;
    }


    public String defaultValueAsString(String name) {
        ConfigKey key = keyRequired(name);

        if (!key.hasDefault()) {
            return "";
        }

        if (key.defaultValue == null) {
            return "null";
        }

        String defaultValueStr = ConfigDef.convertToString(key.defaultValue, key.type);
        if (defaultValueStr.isEmpty()) {
            return "\"\"";
        } else {
            return defaultValueStr;
        }
    }

}
