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
package com.epam.eco.commons.kafka.consumer.bootstrap;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import com.epam.eco.commons.kafka.serde.KeyValueDecoder;

/**
 * @author Andrei_Tytsik
 */
public class ToListDecodingRecordCollector<K, V> implements RecordCollector<byte[], byte[], List<ConsumerRecord<K, V>>> {

    private final KeyValueDecoder<K, V> decoder;

    private volatile List<ConsumerRecord<K, V>> dataList;

    public ToListDecodingRecordCollector(KeyValueDecoder<K, V> decoder) {
        Validate.notNull(decoder, "Decoder is null");

        this.decoder = decoder;
    }

    @Override
    public void collect(ConsumerRecords<byte[], byte[]> records) {
        decodeAndCollectToList(records);
    }

    @Override
    public List<ConsumerRecord<K, V>> result() {
        try {
            return getDataList();
        } finally {
            resetDataList();;
        }
    }

    private void decodeAndCollectToList(ConsumerRecords<byte[], byte[]> records) {
        records.forEach(record -> {
           K key = decodeKey(record.key());
           V value = decodeValue(key, record.value());
           getDataList().add(
                   new ConsumerRecord<>(
                           record.topic(),
                           record.partition(),
                           record.offset(),
                           key,
                           value));
        });
    }

    private K decodeKey(byte[] keyBytes) {
        return decoder.decodeKey(keyBytes);
    }

    private V decodeValue(K key, byte[] valueBytes) {
        return decoder.decodeValue(key, valueBytes);
    }

    private List<ConsumerRecord<K, V>> getDataList() {
        if (dataList == null) {
            dataList = new ArrayList<>();
        }
        return dataList;
    }

    private void resetDataList() {
        dataList = null;
    }

}
