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
package com.epam.eco.commons.kafka.consumer.bootstrap;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import com.epam.eco.commons.kafka.serde.KeyValueDecoder;

/**
 * @author Andrei_Tytsik
 */
public class ToMapDecodingRecordCollector<K, V> implements RecordCollector<byte[], byte[], Map<K, V>> {

    private final KeyValueDecoder<K, V> decoder;

    private Map<K, V> dataMap;

    public ToMapDecodingRecordCollector(KeyValueDecoder<K, V> decoder) {
        Validate.notNull(decoder, "Decoder is null");

        this.decoder = decoder;
    }

    @Override
    public void collect(ConsumerRecords<byte[], byte[]> records) {
        collectToMapAsDecodedKeyRawValuePairs(records);
    }

    @Override
    public Map<K, V> result() {
        try {
            return getDataMap();
        } finally {
            resetDataMap();
        }
    }

    private void collectToMapAsDecodedKeyRawValuePairs(ConsumerRecords<byte[], byte[]> records) {
        records.forEach(consumerRecord -> getDataMap().put(decodeKey(consumerRecord.key()),
                decodeValue(consumerRecord)));
    }

    private K decodeKey(byte[] keyBytes) {
        return decoder.decodeKey(keyBytes);
    }

    private V decodeValue(ConsumerRecord<byte[], byte[]> consumerRecord) {
        return decoder.decodeRecord(consumerRecord);
    }

    private Map<K, V> getDataMap() {
        if (dataMap == null) {
            dataMap = new HashMap<>();
        }
        return dataMap;
    }

    private void resetDataMap() {
        dataMap = null;
    }

}
