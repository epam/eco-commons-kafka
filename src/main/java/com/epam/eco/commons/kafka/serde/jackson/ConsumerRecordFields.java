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
package com.epam.eco.commons.kafka.serde.jackson;

/**
 * @author Andrei_Tytsik
 */
public abstract class ConsumerRecordFields {

    public static final String TOPIC = "topic";
    public static final String PARTITION = "partition";
    public static final String OFFSET = "offset";
    public static final String TIMESTAMP = "timestamp";
    public static final String TIMESTAMP_TYPE = "timestampType";
    public static final String SERIALIZED_KEY_SIZE = "serializedKeySize";
    public static final String SERIALIZED_VALUE_SIZE = "serializedValueSize";
    public static final String HEADERS = "headers";
    public static final String KEY = "key";
    public static final String KEY_CLASS = "@keyClass";
    public static final String VALUE = "value";
    public static final String VALUE_CLASS = "@valueClass";
    public static final String CHECKSUM = "checksum";

    private ConsumerRecordFields() {
    }

}
