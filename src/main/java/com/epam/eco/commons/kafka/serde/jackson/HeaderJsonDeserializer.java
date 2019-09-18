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

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

/**
 * @author Raman_Babich
 */
public class HeaderJsonDeserializer extends StdDeserializer<Header> {

    private static final long serialVersionUID = 1L;

    public HeaderJsonDeserializer() {
        super(Header.class);
    }

    @Override
    public Header deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        ObjectCodec oc = p.getCodec();
        JsonNode node = oc.readTree(p);
        if (node == null || node.isNull()) {
            return null;
        }

        String key = JsonDeserializerUtils.readFieldAsString(node, HeaderFields.KEY, false, ctxt);
        byte[] value = JsonDeserializerUtils.readFieldAsBinary(node, HeaderFields.VALUE, true, ctxt);
        return new RecordHeader(key, value);
    }

}
