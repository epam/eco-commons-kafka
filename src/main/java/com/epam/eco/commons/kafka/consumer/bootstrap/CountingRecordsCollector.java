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
package com.epam.eco.commons.kafka.consumer.bootstrap;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * @author Andrei_Tytsik
 */
public class CountingRecordsCollector<K, V> implements RecordCollector<K, V, Long> {

    private final AtomicLong count = new AtomicLong(0);

    @Override
    public void collect(ConsumerRecords<K, V> records) {
        count.addAndGet(records.count());
    }

    @Override
    public Long result() {
        try {
            return count.get();
        } finally {
            count.set(0);
        }
    }

}
