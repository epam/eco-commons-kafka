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

import java.io.IOException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.ContextualSerializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.fasterxml.jackson.databind.type.SimpleType;

/**
 * @author Raman_Babich
 */
@SuppressWarnings("rawtypes")
public class ConsumerRecordJsonSerializer extends StdSerializer<ConsumerRecord> implements ContextualSerializer {

    private static final long serialVersionUID = 1L;

    private static final JavaType JAVA_OBJECT_TYPE = SimpleType.constructUnsafe(Object.class);
    private JavaType keyType = JAVA_OBJECT_TYPE;
    private JavaType valueType = JAVA_OBJECT_TYPE;

    public ConsumerRecordJsonSerializer() {
        super(ConsumerRecord.class);
    }

    @Override
    public JsonSerializer<?> createContextual(
            SerializerProvider prov, BeanProperty property) throws JsonMappingException {
        JavaType recordType;
        if (property == null) {
            return new ConsumerRecordJsonSerializer();
        }
        recordType = property.getType();
        ConsumerRecordJsonSerializer serializer = new ConsumerRecordJsonSerializer();
        if (recordType.hasGenericTypes()) {
            serializer.keyType = recordType.containedType(0);
            serializer.valueType = recordType.containedType(1);
        }
        return serializer;
    }

    @SuppressWarnings({"deprecation"})
    @Override
    public void serialize(
            ConsumerRecord value,
            JsonGenerator gen,
            SerializerProvider serializers) throws IOException {
        gen.writeStartObject();
        gen.writeStringField(ConsumerRecordFields.TOPIC, value.topic());
        gen.writeNumberField(ConsumerRecordFields.PARTITION, value.partition());
        gen.writeNumberField(ConsumerRecordFields.OFFSET, value.offset());
        gen.writeNumberField(ConsumerRecordFields.TIMESTAMP, value.timestamp());
        gen.writeObjectField(ConsumerRecordFields.TIMESTAMP_TYPE, value.timestampType());
        gen.writeNumberField(ConsumerRecordFields.CHECKSUM, value.checksum());
        gen.writeNumberField(ConsumerRecordFields.SERIALIZED_KEY_SIZE, value.serializedKeySize());
        gen.writeNumberField(ConsumerRecordFields.SERIALIZED_VALUE_SIZE, value.serializedValueSize());
        if (keyType.isJavaLangObject()) {
            gen.writeObjectField(ConsumerRecordFields.KEY_CLASS,
                    value.key() != null ? value.key().getClass() : keyType);
        } else {
            gen.writeObjectField(ConsumerRecordFields.KEY_CLASS, keyType);
        }
        gen.writeObjectField(ConsumerRecordFields.KEY, value.key());
        if (valueType.isJavaLangObject()) {
            gen.writeObjectField(ConsumerRecordFields.VALUE_CLASS,
                    value.value() != null ? value.value().getClass() : valueType);
        } else {
            gen.writeObjectField(ConsumerRecordFields.VALUE_CLASS, valueType);
        }
        gen.writeObjectField(ConsumerRecordFields.VALUE, value.value());
        gen.writeObjectField(ConsumerRecordFields.HEADERS, value.headers());
        gen.writeEndObject();
    }

}
