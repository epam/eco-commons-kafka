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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

import com.fasterxml.jackson.databind.module.SimpleModule;

/**
 * @author Andrei_Tytsik
 */
public class KafkaModule extends SimpleModule {

    private static final long serialVersionUID = 1L;

    public KafkaModule() {
        super(KafkaModule.class.getSimpleName());

        addKeyDeserializer(TopicPartition.class, new TopicPartitionKeyDeserializer());

        addSerializer(new RecordHeadersJsonSerializer());
        addSerializer(new RecordHeaderJsonSerializer());
        addSerializer(new ConsumerRecordJsonSerializer());
        addSerializer(new TopicPartitionJsonSerializer());
        addSerializer(new KafkaPrincipalJsonSerializer());

        addDeserializer(RecordHeader.class, new RecordHeaderJsonDeserializer());
        addDeserializer(RecordHeaders.class, new RecordHeadersJsonDeserializer());
        addDeserializer(Header.class, new RecordHeaderJsonDeserializer(Header.class));
        addDeserializer(Headers.class, new RecordHeadersJsonDeserializer(Headers.class));
        addDeserializer(ConsumerRecord.class, new ConsumerRecordJsonDeserializer());
        addDeserializer(TopicPartition.class, new TopicPartitionJsonDeserializer());
        addDeserializer(KafkaPrincipal.class, new KafkaPrincipalJsonDeserializer());
    }

}
