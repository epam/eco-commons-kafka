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
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.epam.eco.commons.kafka.config.ConsumerConfigBuilder;
import com.epam.eco.commons.kafka.consumer.bootstrap.BootstrapConsumer;
import com.epam.eco.commons.kafka.consumer.bootstrap.BootstrapConsumerExecutor;
import com.epam.eco.commons.kafka.consumer.bootstrap.OffsetInitializer;
import com.epam.eco.commons.kafka.consumer.bootstrap.ToListDecodingRecordCollector;
import com.epam.eco.commons.kafka.consumer.bootstrap.ToListRecordCollector;
import com.epam.eco.commons.kafka.serde.KeyValueDecoder;

/**
 * @author Andrei_Tytsik
 */
class ListCacheConsumer<K, V> implements Closeable {

    private final BootstrapConsumerExecutor<?, ?, Map<K, V>> consumerExecutor;

    public ListCacheConsumer(
            String topicName,
            String bootstrapServers,
            long bootstrapTimeoutInMs,
            Map<String, Object> consumerConfig,
            OffsetInitializer offsetInitializer,
            KeyValueDecoder<K, V> keyValueDecoder,
            int parallelism,
            Consumer<List<ConsumerRecord<K, V>>> consumerHandler) {
        consumerExecutor = initExecutor(
                topicName,
                bootstrapServers,
                bootstrapTimeoutInMs,
                consumerConfig,
                offsetInitializer,
                keyValueDecoder,
                parallelism,
                consumerHandler);
    }

    public void start() throws InterruptedException {
        startExecutorAndWaitForBootstrap();
    }

    @Override
    public void close() {
        destroyExecutor();
    }

    private void startExecutorAndWaitForBootstrap() throws InterruptedException {
        consumerExecutor.start();
        consumerExecutor.waitForBootstrap();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private BootstrapConsumerExecutor<?, ?, Map<K, V>> initExecutor(
            String topicName,
            String bootstrapServers,
            long bootstrapTimeoutInMs,
            Map<String, Object> consumerConfig,
            OffsetInitializer offsetInitializer,
            KeyValueDecoder<K, V> keyValueDecoder,
            int parallelism,
            Consumer<List<ConsumerRecord<K, V>>> consumerHandler) {
        BootstrapConsumer.Builder<K, V, List<ConsumerRecord<K, V>>> consumerBuilder =
                BootstrapConsumer.<K, V, List<ConsumerRecord<K, V>>>builder().
                    topicName(topicName).
                    consumerConfig(ConsumerConfigBuilder.
                            with(consumerConfig).
                            bootstrapServers(bootstrapServers).
                            build()).
                    bootstrapTimeoutInMs(bootstrapTimeoutInMs);
        if (offsetInitializer != null) {
            consumerBuilder.offsetInitializer(offsetInitializer);
        }

        Supplier recordCollectorSupplier = () ->
                keyValueDecoder != null ?
                new ToListDecodingRecordCollector(keyValueDecoder) :
                new ToListRecordCollector();

        return new BootstrapConsumerExecutor<>(
                consumerBuilder,
                recordCollectorSupplier,
                parallelism,
                consumerHandler);
    }

    private void destroyExecutor() {
        try {
            consumerExecutor.shutdown();
        } catch (InterruptedException ie) {
            throw new RuntimeException(ie);
        }
    }

}
