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

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * @author Andrei_Tytsik
 */
public class ToMapRecordCollector<K, V> implements RecordCollector<K, V, Map<K, V>> {

    private volatile Map<K, V> dataMap;

    @Override
    public void collect(ConsumerRecords<K, V> records) {
        collectToMap(records);
    }

    @Override
    public Map<K, V> result() {
        try {
            return getDataMap();
        } finally {
            resetDataMap();
        }
    }

    protected void collectToMap(ConsumerRecords<K, V> records) {
        records.forEach(record -> getDataMap().put(record.key(), record.value()));
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
