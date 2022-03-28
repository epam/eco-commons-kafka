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
package com.epam.eco.commons.kafka.cache;

import java.io.Closeable;
import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.epam.eco.commons.kafka.config.ProducerConfigBuilder;

/**
 * @author Andrei_Tytsik
 */
class CacheProducer<K, V> implements Closeable {

    private static final int LINGER = 20;

    private final String topicName;
    private final KafkaProducer<K, V> producer;

    public CacheProducer(
            String topicName,
            String bootstrapServers,
            Map<String, Object> producerConfig) {
        this.topicName = topicName;
        producer = initProducer(bootstrapServers, producerConfig);
    }

    @Override
    public void close() {
        producer.close();
    }

    public void send(Map<K, V> update) {
        update.entrySet().forEach(entry -> {
            ProducerRecord<K, V> record =
                    new ProducerRecord<>(topicName, entry.getKey(), entry.getValue());
            producer.send(record, null);
        });

        producer.flush();
    }

    private static <K, V> KafkaProducer<K, V> initProducer(
            String bootstrapServers,
            Map<String, Object> producerConfig) {
        producerConfig = ProducerConfigBuilder.
                with(producerConfig).
                bootstrapServers(bootstrapServers).
                lingerMs(LINGER).
                acksAll().
                retriesMax().
                maxBlockMsMax().
                build();

        return new KafkaProducer<>(producerConfig);
    }

}
