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

import java.io.IOException;

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * @author Andrei_Tytsik
 */
public class JsonDeserializerUtils {

    public static byte[] readFieldAsBinary(
            JsonNode node,
            String fieldName,
            boolean nullable,
            DeserializationContext context) throws IOException {
        JsonNode fieldNode = node.get(fieldName);

        if (fieldNode == null || fieldNode.isNull()) {
            if (nullable) {
                return null;
            } else {
                context.reportInputMismatch(byte[].class, "% is null", fieldName);
            }
        }

        return fieldNode.binaryValue();
    }

    public static String readFieldAsString(
            JsonNode node,
            String fieldName,
            boolean nullable,
            DeserializationContext context) throws JsonMappingException {
        JsonNode fieldNode = node.get(fieldName);

        if (fieldNode == null || fieldNode.isNull()) {
            if (nullable) {
                return null;
            } else {
                context.reportInputMismatch(String.class, "% is null", fieldName);
            }
        }

        return fieldNode.asText();
    }

    public static Integer readFieldAsInteger(
            JsonNode node,
            String fieldName,
            boolean nullable,
            DeserializationContext context) throws JsonMappingException {
        JsonNode fieldNode = node.get(fieldName);

        if (fieldNode == null || fieldNode.isNull()) {
            if (nullable) {
                return null;
            } else {
                context.reportInputMismatch(Integer.class, "% is null", fieldName);
            }
        }

        if (!fieldNode.canConvertToInt()) {
            context.reportInputMismatch(Integer.class, "% is invalid", fieldName);
        }

        return fieldNode.asInt();
    }

    public static Long readFieldAsLong(
            JsonNode node,
            String fieldName,
            boolean nullable,
            DeserializationContext context) throws JsonMappingException {
        JsonNode fieldNode = node.get(fieldName);

        if (fieldNode == null || fieldNode.isNull()) {
            if (nullable) {
                return null;
            } else {
                context.reportInputMismatch(Long.class, "% is null", fieldName);
            }
        }

        if (!fieldNode.canConvertToLong()) {
            context.reportInputMismatch(Long.class, "% is invalid", fieldName);
        }

        return fieldNode.asLong();
    }

}
