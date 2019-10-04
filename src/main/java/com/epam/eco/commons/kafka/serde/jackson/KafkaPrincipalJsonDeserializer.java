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

import org.apache.kafka.common.security.auth.KafkaPrincipal;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

/**
 * @author Raman_Babich
 */
public class KafkaPrincipalJsonDeserializer extends StdDeserializer<KafkaPrincipal> {

    private static final long serialVersionUID = 1L;

    public KafkaPrincipalJsonDeserializer() {
        super(KafkaPrincipal.class);
    }

    @Override
    public KafkaPrincipal deserialize(JsonParser jsonParser, DeserializationContext ctxt) throws IOException {
        if (jsonParser.getCurrentToken() == JsonToken.START_OBJECT) {
            jsonParser.nextToken();
        }
        String fieldName = jsonParser.getCurrentName();

        String principalType = null;
        String name = null;
        while (fieldName != null) {
            if (KafkaPrincipalFields.PRINCIPAL_TYPE.equals(fieldName)) {
                jsonParser.nextToken();
                principalType = _parseString(jsonParser, ctxt);
            } else if (KafkaPrincipalFields.NAME.equals(fieldName)) {
                jsonParser.nextToken();
                name = _parseString(jsonParser, ctxt);
            } else {
                handleUnknownProperty(jsonParser, ctxt, _valueClass, fieldName);
            }
            fieldName = jsonParser.nextFieldName();
        }
        return new KafkaPrincipal(principalType, name);
    }

}
