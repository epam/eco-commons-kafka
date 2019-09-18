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

import org.apache.commons.lang3.ClassUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.record.TimestampType;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

/**
 * @author Raman_Babich
 */
@SuppressWarnings("rawtypes")
public class ConsumerRecordJsonDeserializer extends StdDeserializer<ConsumerRecord> {

    private static final long serialVersionUID = 1L;

    public ConsumerRecordJsonDeserializer() {
        super(ConsumerRecord.class);
    }

    @Override
    public ConsumerRecord deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        ObjectCodec oc = p.getCodec();
        JsonNode node = oc.readTree(p);
        if (node == null || node.isNull()) {
            return null;
        }

        try {
            String topic =
                    JsonDeserializerUtils.readFieldAsString(node, ConsumerRecordFields.TOPIC, false, ctxt);
            Integer partition =
                    JsonDeserializerUtils.readFieldAsInteger(node, ConsumerRecordFields.PARTITION, false, ctxt);
            Long offset =
                    JsonDeserializerUtils.readFieldAsLong(node, ConsumerRecordFields.OFFSET, false, ctxt);
            Long timestamp =
                    JsonDeserializerUtils.readFieldAsLong(node, ConsumerRecordFields.TIMESTAMP, false, ctxt);

            String timestampTypeStr =
                    JsonDeserializerUtils.readFieldAsString(node, ConsumerRecordFields.TIMESTAMP_TYPE, true, ctxt);
            TimestampType timestampType =
                    timestampTypeStr != null ? TimestampType.forName(timestampTypeStr) : null;

            Long checksum =
                    JsonDeserializerUtils.readFieldAsLong(node, ConsumerRecordFields.CHECKSUM, true, ctxt);

            Integer serializedKeySize =
                    JsonDeserializerUtils.readFieldAsInteger(
                            node, ConsumerRecordFields.SERIALIZED_KEY_SIZE, false, ctxt);
            Integer serializedValueSize =
                    JsonDeserializerUtils.readFieldAsInteger(
                            node, ConsumerRecordFields.SERIALIZED_VALUE_SIZE, false, ctxt);

            Object key = null;
            JsonNode keyNode = node.get(ConsumerRecordFields.KEY);
            if (keyNode != null && !keyNode.isNull()) {
                String keyClass =
                        JsonDeserializerUtils.readFieldAsString(
                                node, ConsumerRecordFields.KEY_CLASS, false, ctxt);;
                key = oc.readValue(keyNode.traverse(oc), ClassUtils.getClass(keyClass));
            }

            Object value = null;
            JsonNode valueNode = node.get(ConsumerRecordFields.VALUE);
            if (valueNode != null && !valueNode.isNull()) {
                String valueClass =
                        JsonDeserializerUtils.readFieldAsString(
                                node, ConsumerRecordFields.VALUE_CLASS, false, ctxt);;
                value = oc.readValue(valueNode.traverse(oc), ClassUtils.getClass(valueClass));
            }

            Headers headers = null;
            JsonNode headersNode = node.get(ConsumerRecordFields.HEADERS);
            if (headersNode != null && !headersNode.isNull()) {
                headers = oc.readValue(headersNode.traverse(oc), Headers.class);
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
        } catch (ClassNotFoundException cnfe) {
            throw new JsonMappingException(
                    null,
                    String.format(
                            "Error has been encountered while parsing '%s' as ConsumerRecord.", node.toString()),
                    cnfe);
        }
    }

}
