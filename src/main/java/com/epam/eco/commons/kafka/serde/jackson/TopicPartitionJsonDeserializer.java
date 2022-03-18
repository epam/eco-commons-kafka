/*******************************************************************************
 *  Copyright 2022 EPAM Systems
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License.  You may obtain a copy
 *  of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 *******************************************************************************/
package com.epam.eco.commons.kafka.serde.jackson;

import java.io.IOException;

import org.apache.kafka.common.TopicPartition;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

/**
 * @author Raman_Babich
 */
public class TopicPartitionJsonDeserializer extends StdDeserializer<TopicPartition> {

    private static final long serialVersionUID = 1L;

    public TopicPartitionJsonDeserializer() {
        super(TopicPartition.class);
    }

    @Override
    public TopicPartition deserialize(
            JsonParser jsonParser,
            DeserializationContext ctxt) throws IOException {
        if (jsonParser.getCurrentToken() == JsonToken.START_OBJECT) {
            jsonParser.nextToken();
        }
        String fieldName = jsonParser.getCurrentName();

        String topic = null;
        Integer partition = null;
        while (fieldName != null) {
            if (TopicPartitionFields.TOPIC.equals(fieldName)) {
                jsonParser.nextToken();
                JsonToken currentToken = jsonParser.getCurrentToken();
                if (currentToken == JsonToken.VALUE_STRING || currentToken == JsonToken.VALUE_NULL) {
                    topic = jsonParser.getValueAsString();
                } else {
                    ctxt.reportInputMismatch(
                            _valueClass,
                            "Can't parse string value for '%s' field from '%s' token",
                            TopicPartitionFields.TOPIC, currentToken.name());
                }
            } else if (TopicPartitionFields.PARTITION.equals(fieldName)) {
                jsonParser.nextToken();
                partition = _parseIntPrimitive(jsonParser, ctxt);
            } else {
                handleUnknownProperty(jsonParser, ctxt, _valueClass, fieldName);
            }
            fieldName = jsonParser.nextFieldName();
        }

        com.epam.eco.commons.json.JsonDeserializerUtils.assertRequiredField(partition, TopicPartitionFields.PARTITION, _valueClass, ctxt);

        return new TopicPartition(topic, partition);
    }

}
