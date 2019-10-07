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
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.record.TimestampType;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.deser.ContextualDeserializer;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.type.SimpleType;

/**
 * @author Raman_Babich
 */
@SuppressWarnings("rawtypes")
public class ConsumerRecordJsonDeserializer extends StdDeserializer<ConsumerRecord> implements ContextualDeserializer {

    private static final long serialVersionUID = 1L;

    private static final JavaType JAVA_OBJECT_TYPE = SimpleType.constructUnsafe(Object.class);
    private JavaType keyType = JAVA_OBJECT_TYPE;
    private JavaType valueType = JAVA_OBJECT_TYPE;

    public ConsumerRecordJsonDeserializer() {
        super(ConsumerRecord.class);
    }

    @Override
    public JsonDeserializer<?> createContextual(
            DeserializationContext ctxt,
            BeanProperty property) throws JsonMappingException {
        JavaType recordType;
        if (property != null) {
            recordType = property.getType();
        } else {
            recordType = ctxt.getContextualType();
        }
        ConsumerRecordJsonDeserializer deserializer = new ConsumerRecordJsonDeserializer();
        if (recordType.hasGenericTypes()) {
            deserializer.keyType = recordType.containedType(0);
            deserializer.valueType = recordType.containedType(1);
        }
        return deserializer;
    }


    // TODO: simplify this method
    @Override
    public ConsumerRecord deserialize(JsonParser jsonParser, DeserializationContext ctxt) throws IOException {
        if (jsonParser.getCurrentToken() == JsonToken.START_OBJECT) {
            jsonParser.nextToken();
        }
        String fieldName = jsonParser.getCurrentName();

        String topic = null;
        Integer partition = null;
        Long offset = null;
        Long timestamp = null;
        TimestampType timestampType = null;
        Long checksum = null;
        Integer serializedKeySize = null;
        Integer serializedValueSize = null;
        JavaType keyClass = JAVA_OBJECT_TYPE;
        TreeNode keyNode = null;
        Object key = null;
        JavaType valueClass = JAVA_OBJECT_TYPE;
        TreeNode valueNode = null;
        Object value = null;
        Headers headers = null;

        while (fieldName != null) {
            switch (fieldName) {
                case ConsumerRecordFields.TOPIC:
                    jsonParser.nextToken();
                    topic = _parseString(jsonParser, ctxt);
                    break;
                case ConsumerRecordFields.PARTITION:
                    jsonParser.nextToken();
                    partition = _parseIntPrimitive(jsonParser, ctxt);
                    break;
                case ConsumerRecordFields.OFFSET:
                    jsonParser.nextToken();
                    offset = _parseLongPrimitive(jsonParser, ctxt);
                    break;
                case ConsumerRecordFields.TIMESTAMP:
                    jsonParser.nextToken();
                    timestamp = _parseLongPrimitive(jsonParser, ctxt);
                    break;
                case ConsumerRecordFields.TIMESTAMP_TYPE:
                    jsonParser.nextToken();
                    timestampType = jsonParser.readValueAs(TimestampType.class);
                    break;
                case ConsumerRecordFields.CHECKSUM:
                    jsonParser.nextToken();
                    checksum = _parseLongPrimitive(jsonParser, ctxt);
                    break;
                case ConsumerRecordFields.SERIALIZED_KEY_SIZE:
                    jsonParser.nextToken();
                    serializedKeySize = _parseIntPrimitive(jsonParser, ctxt);
                    break;
                case ConsumerRecordFields.SERIALIZED_VALUE_SIZE:
                    jsonParser.nextToken();
                    serializedValueSize = _parseIntPrimitive(jsonParser, ctxt);
                    break;
                case ConsumerRecordFields.KEY_CLASS:
                    jsonParser.nextToken();
                    if (jsonParser.getCurrentToken() != JsonToken.VALUE_NULL) {
                        keyClass = jsonParser.readValueAs(JavaType.class);
                    }
                    break;
                case ConsumerRecordFields.KEY:
                    jsonParser.nextToken();
                    keyNode = jsonParser.getCodec().readTree(jsonParser);
                    break;
                case ConsumerRecordFields.VALUE_CLASS:
                    jsonParser.nextToken();
                    if (jsonParser.getCurrentToken() != JsonToken.VALUE_NULL) {
                        valueClass = jsonParser.readValueAs(JavaType.class);
                    }
                    break;
                case ConsumerRecordFields.VALUE:
                    jsonParser.nextToken();
                    valueNode = jsonParser.getCodec().readTree(jsonParser);
                    break;
                case ConsumerRecordFields.HEADERS:
                    jsonParser.nextToken();
                    headers = jsonParser.readValueAs(Headers.class);
                    break;
                default:
                    handleUnknownProperty(jsonParser, ctxt, _valueClass, fieldName);
                    break;
            }
            fieldName = jsonParser.nextFieldName();
        }

        validateNotNull(partition, ConsumerRecordFields.PARTITION, ctxt);
        validateNotNull(offset, ConsumerRecordFields.OFFSET, ctxt);

        ObjectCodec codec = jsonParser.getCodec();
        if (keyNode != null) {
            JavaType targetType = valueType;
            if (keyType.isJavaLangObject()) {
                targetType = keyClass;
            }
            key = codec.readValue(keyNode.traverse(codec), targetType);

        }
        if (valueNode != null) {
            JavaType targetType = valueType;
            if (valueType.isJavaLangObject()) {
                targetType = valueClass;
            }
            value = codec.readValue(valueNode.traverse(codec), targetType);
        }

        if (
                timestamp == null &&
                timestampType == null &&
                checksum == null &&
                serializedKeySize == null &&
                serializedValueSize == null &&
                headers == null
        ) {
            return new ConsumerRecord<>(topic, partition, offset, key, value);
        }

        validateNotNull(timestamp, ConsumerRecordFields.TIMESTAMP, ctxt);
        validateNotNull(checksum, ConsumerRecordFields.CHECKSUM, ctxt);
        validateNotNull(serializedKeySize, ConsumerRecordFields.SERIALIZED_KEY_SIZE, ctxt);
        validateNotNull(serializedValueSize, ConsumerRecordFields.SERIALIZED_VALUE_SIZE, ctxt);

        if (headers == null) {
            return new ConsumerRecord<>(
                    topic,
                    partition,
                    offset,
                    timestamp,
                    timestampType,
                    checksum,
                    serializedKeySize,
                    serializedValueSize,
                    key,
                    value);
        }
        return new ConsumerRecord<>(
                topic,
                partition,
                offset,
                timestamp,
                timestampType,
                checksum,
                serializedKeySize,
                serializedValueSize,
                key,
                value,
                headers);
    }

    private void validateNotNull(
            Object value, String fieldName, DeserializationContext ctxt) throws JsonMappingException {
        if (value == null) {
            ctxt.reportInputMismatch(_valueClass, "Field '%s' is required", fieldName);
        }
    }

}
