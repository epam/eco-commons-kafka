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
import java.util.List;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

/**
 * @author Raman_Babich
 */
public class HeadersJsonDeserializer extends StdDeserializer<Headers> {

    private static final long serialVersionUID = 1L;

    public HeadersJsonDeserializer() {
        super(Headers.class);
    }

    @Override
    public Headers deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        ObjectCodec oc = p.getCodec();
        JsonNode node = oc.readTree(p);
        if (node == null || node.isNull()) {
            return null;
        }

        List<Header> headers = oc.readValue(node.traverse(oc), new TypeReference<List<Header>>(){});
        return new RecordHeaders(headers);
    }

}
