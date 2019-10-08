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
import com.fasterxml.jackson.databind.PropertyName;
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
        ConsumerRecordJsonSerializer serializer = new ConsumerRecordJsonSerializer();
        if (property == null) {
            return serializer;
        }
        JavaType recordType = traverseByContentTypes(property.getType(), ConsumerRecord.class);
        if (recordType == null) {
            prov.reportBadDefinition(
                    property.getType(),
                    String.format(
                            "Can't identify any type parts which are associated with '%s' class",
                            ConsumerRecord.class.getName()));
        }
        if (recordType.hasGenericTypes()) {
            serializer.keyType = recordType.containedType(0);
            serializer.valueType = recordType.containedType(1);
        }
        return serializer;
    }

    private static JavaType traverseByContentTypes(JavaType root, Class<?> stopClass) {
        JavaType currentType = root;
        while (true) {
            if (currentType.hasRawClass(stopClass)) {
                return currentType;
            }

            if (currentType.isCollectionLikeType() || currentType.isMapLikeType()) {
                currentType = currentType.getContentType();
            } else {
                return null;
            }
        }
    }

    private static BeanProperty typeHolderBeanProperty(JavaType type) {
        return new BeanProperty.Std(PropertyName.NO_NAME, type, PropertyName.NO_NAME, null, null);
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

        JavaType effectiveKeyType = keyType;
        if (keyType.isJavaLangObject()) {
            if (value.key() != null) {
                effectiveKeyType = SimpleType.constructUnsafe(value.key().getClass());
            }
        }
        gen.writeObjectField(ConsumerRecordFields.KEY_CLASS, effectiveKeyType);
        BeanProperty keyProp = typeHolderBeanProperty(effectiveKeyType);
        JsonSerializer<Object> keySerializer = serializers.findValueSerializer(effectiveKeyType, keyProp);
        gen.writeFieldName(ConsumerRecordFields.KEY);
        keySerializer.serialize(value.key(), gen, serializers);

        JavaType effectiveValueType = valueType;
        if (valueType.isJavaLangObject()) {
            if (value.value() != null) {
                effectiveValueType = SimpleType.constructUnsafe(value.value().getClass());
            }
        }
        gen.writeObjectField(ConsumerRecordFields.VALUE_CLASS, effectiveValueType);
        BeanProperty valueProp = typeHolderBeanProperty(effectiveValueType);
        JsonSerializer<Object> valueSerializer = serializers.findValueSerializer(effectiveValueType, valueProp);
        gen.writeFieldName(ConsumerRecordFields.VALUE);
        valueSerializer.serialize(value.value(), gen, serializers);

        gen.writeObjectField(ConsumerRecordFields.HEADERS, value.headers());
        gen.writeEndObject();
    }

}
