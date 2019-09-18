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

import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.epam.eco.commons.kafka.config.ConsumerConfigBuilder;
import com.epam.eco.commons.kafka.consumer.bootstrap.BootstrapConsumer;
import com.epam.eco.commons.kafka.consumer.bootstrap.TimestampOffsetInitializer;
import com.epam.eco.commons.kafka.consumer.bootstrap.ToListRecordCollector;


/**
 * @author Andrei_Tytsik
 */
public class BootstrapConsumerIT {

    @SuppressWarnings("unused")
    private static final Logger LOGGER = LoggerFactory.getLogger(BootstrapConsumerIT.class);

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC_NAME = "__consumer_offsets";

    public static void main(String[] args) {
        BootstrapConsumer<byte[], byte[], List<ConsumerRecord<byte[], byte[]>>> consumer =
                BootstrapConsumer.<byte[], byte[], List<ConsumerRecord<byte[], byte[]>>>builder().
                        topicName(TOPIC_NAME).
                        consumerConfig(ConsumerConfigBuilder.withEmpty().
                            bootstrapServers(BOOTSTRAP_SERVERS).
                            keyDeserializerByteArray().
                            valueDeserializerByteArray().
                            build()).
                        offsetInitializer(TimestampOffsetInitializer.oneDayBefore()).
                        bootstrapTimeoutInMs(5 * 60 * 1000).
                        recordCollector(new ToListRecordCollector<>()).
                        build();

        List<ConsumerRecord<byte[], byte[]>> result = consumer.fetch();
        Assert.assertFalse(result.isEmpty());

        consumer.close();
    }

}
