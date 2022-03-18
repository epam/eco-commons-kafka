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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

/**
 * @author Raman_Babich
 */
public class RecordHeadersJsonDeserializer extends StdDeserializer<RecordHeaders> {

    private static final long serialVersionUID = 1L;

    public RecordHeadersJsonDeserializer() {
        super(RecordHeaders.class);
    }

    public RecordHeadersJsonDeserializer(Class<?> vc) {
        super(vc);
    }

    @Override
    public RecordHeaders deserialize(JsonParser jsonParser, DeserializationContext ctxt) throws IOException {
        if (jsonParser.getCurrentToken() != JsonToken.START_ARRAY) {
            ctxt.reportWrongTokenException(
                    _valueClass,
                    jsonParser.getCurrentToken(),
                    "Input should start with '%s' token", JsonToken.START_ARRAY.asString());
        }
        jsonParser.nextToken();
        if (jsonParser.getCurrentToken() == JsonToken.END_ARRAY) {
            return new RecordHeaders();
        }
        List<Header> headers = toList(jsonParser.readValuesAs(Header.class));
        return new RecordHeaders(headers);
    }

    private static  <T> List<T> toList(Iterator<T> iterator) {
        List<T> list = new ArrayList<>();
        while (iterator.hasNext()) {
            list.add(iterator.next());
        }
        return list;
    }

}
