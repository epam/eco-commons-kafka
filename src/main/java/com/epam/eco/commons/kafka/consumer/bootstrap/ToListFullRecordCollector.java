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

import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * @author Aliaksei_Valyaev
 */
public class ToListFullRecordCollector<K, V> extends ToListRecordCollector<K, V> {

    protected void collectToList(ConsumerRecords<K, V> records) {
        records.forEach(record -> getDataList().add(record));
    }
}
