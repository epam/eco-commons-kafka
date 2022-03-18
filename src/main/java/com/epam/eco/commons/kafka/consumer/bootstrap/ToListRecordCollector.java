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

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * @author Andrei_Tytsik
 */
public class ToListRecordCollector<K, V> implements RecordCollector<K, V, List<ConsumerRecord<K, V>>> {

    private volatile List<ConsumerRecord<K, V>> dataList;

    @Override
    public void collect(ConsumerRecords<K, V> records) {
        collectToList(records);
    }

    @Override
    public List<ConsumerRecord<K, V>> result() {
        try {
            return getDataList();
        } finally {
            resetDataList();
        }
    }

    protected void collectToList(ConsumerRecords<K, V> records) {
        records.forEach(record -> getDataList().add(
                new ConsumerRecord<>(
                        record.topic(),
                        record.partition(),
                        record.offset(),
                        record.key(),
                        record.value())));
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
