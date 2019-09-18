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
package com.epam.eco.commons.kafka.producer.tx;

import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.epam.eco.commons.kafka.config.ProducerConfigBuilder;

import static java.lang.String.format;

/**
 * @author Aliaksei_Valyaev
 */
public final class KafkaTxProducerFactory<K, V> implements TxProducerFactory<K, V> {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    @Override
    public Producer<K, V> create(
            String idPrefix,
            TopicPartition inTopic,
            Map<String, Object> cfg
    ) {
        return new KafkaProducer<>(buildCfg(idPrefix, inTopic, cfg));
    }

    private Map<String, Object> buildCfg(
            String idPrefix,
            TopicPartition inTopic,
            Map<String, Object> origin
    ) {
        String txId = format("%s.%s.%s", idPrefix, inTopic.topic(), inTopic.partition());
        log.info("creating producer with txId {}", txId);

        return ProducerConfigBuilder.with(origin).
                transactionalId(txId).
                build();
    }

}
