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
package com.epam.eco.commons.kafka.cache;

import java.util.Map;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.epam.eco.commons.kafka.KafkaUtils;
import com.epam.eco.commons.kafka.config.ConsumerConfigBuilder;
import com.epam.eco.commons.kafka.config.ProducerConfigBuilder;
import com.epam.eco.commons.kafka.config.TopicConfigBuilder;
import com.epam.eco.commons.kafka.consumer.bootstrap.OffsetInitializer;
import com.epam.eco.commons.kafka.serde.KeyValueDecoder;

/**
 * @author Andrei_Tytsik
 */
public class KafkaCache<K, V> extends AbstractCache<K, V> {

    private final static Logger LOGGER = LoggerFactory.getLogger(KafkaCache.class);

    private final CacheTopic topic;
    private final MapCacheConsumer<K, V> consumer;
    private final CacheProducer<K, V> producer;

    private KafkaCache(
            String bootstrapServers,
            String topicName,
            int topicPartitionCount,
            int topicReplicationFactor,
            Map<String, Object> topicConfig,
            long bootstrapTimeoutInMs,
            Map<String, Object> consumerConfig,
            OffsetInitializer offsetInitializer,
            KeyValueDecoder<K, V> keyValueDecoder,
            int consumerParallelism,
            boolean readOnly,
            Map<String, Object> producerConfig,
            boolean storeData,
            FireUpdateMode fireUpdateMode,
            CacheListener<K, V> listener) {
        super(storeData, fireUpdateMode, listener);

        Validate.notBlank(bootstrapServers, "Bootstrap servers list is blank");
        Validate.notBlank(topicName, "Topic name is blank");

        topic = new CacheTopic(
                bootstrapServers,
                topicName,
                topicPartitionCount,
                topicReplicationFactor,
                topicConfig,
                consumerConfig);

        consumer = new MapCacheConsumer<>(
                topicName,
                bootstrapServers,
                bootstrapTimeoutInMs,
                consumerConfig,
                offsetInitializer,
                keyValueDecoder,
                consumerParallelism,
                this::applyUpdateFromUnderlying);

        producer =
                !readOnly ?
                new CacheProducer<>(topicName, bootstrapServers, producerConfig) :
                null;

        LOGGER.info("Initialized");
    }

    private boolean isReadOnly() {
        return producer == null;
    }

    @Override
    public void start() throws Exception {
        if (!isReadOnly()) {
            topic.createIfNotExists();
        }
        consumer.start();

        LOGGER.info("Started");
    }

    @Override
    protected void applyUpdateToUnderlying(Map<K, V> update) {
        if (producer == null) {
            return;
        }

        producer.send(update);
    }

    @Override
    public void close() {
        KafkaUtils.closeQuietly(producer);
        KafkaUtils.closeQuietly(consumer);

        LOGGER.info("Closed");
    }

    public static <K, V> Builder<K, V> builder() {
        return new Builder<>();
    }

    public static class Builder<K, V> {

        private String bootstrapServers;
        private String topicName;
        private int topicPartitionCount = -1;
        private int topicReplicationFactor = -1;
        private Map<String, Object> topicConfig;
        private TopicConfigBuilder topicConfigBuilder;
        private long bootstrapTimeoutInMs = 30000;
        private Map<String, Object> consumerConfig;
        private ConsumerConfigBuilder consumerConfigBuilder;
        private OffsetInitializer offsetInitializer;
        private KeyValueDecoder<K, V> keyValueDecoder;
        private int consumerParallelism = 1;
        private boolean readOnly = true;
        private Map<String, Object> producerConfig;
        private ProducerConfigBuilder producerConfigBuilder;
        private boolean storeData = true;
        private FireUpdateMode fireUpdateMode = FireUpdateMode.EFFECTIVE;
        private CacheListener<K, V> listener;

        public Builder<K, V> bootstrapServers(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
            return this;
        }
        public Builder<K, V> topicName(String topicName) {
            this.topicName = topicName;
            return this;
        }
        public Builder<K, V> topicPartitionCount(int topicPartitionCount) {
            this.topicPartitionCount = topicPartitionCount;
            return this;
        }
        public Builder<K, V> topicReplicationFactor(int topicReplicationFactor) {
            this.topicReplicationFactor = topicReplicationFactor;
            return this;
        }
        public Builder<K, V> topicConfig(Map<String, Object> topicConfig) {
            this.topicConfig = consumerConfig;
            return this;
        }
        public Builder<K, V> topicConfigBuilder(TopicConfigBuilder topicConfigBuilder) {
            this.topicConfigBuilder = topicConfigBuilder;
            return this;
        }
        public Builder<K, V> bootstrapTimeoutInMs(long bootstrapTimeoutInMs) {
            this.bootstrapTimeoutInMs = bootstrapTimeoutInMs;
            return this;
        }
        public Builder<K, V> consumerConfig(Map<String, Object> consumerConfig) {
            this.consumerConfig = consumerConfig;
            return this;
        }
        public Builder<K, V> consumerConfigBuilder(ConsumerConfigBuilder consumerConfigBuilder) {
            this.consumerConfigBuilder = consumerConfigBuilder;
            return this;
        }
        public Builder<K, V> offsetInitializer(OffsetInitializer offsetInitializer) {
            this.offsetInitializer = offsetInitializer;
            return this;
        }
        public Builder<K, V> keyValueDecoder(KeyValueDecoder<K, V> keyValueDecoder) {
            this.keyValueDecoder = keyValueDecoder;
            return this;
        }
        public Builder<K, V> consumerParallelism(int consumerParallelism) {
            this.consumerParallelism = consumerParallelism;
            return this;
        }
        public Builder<K, V> consumerParallelismAvailableCores() {
            this.consumerParallelism = Runtime.getRuntime().availableProcessors();
            return this;
        }
        public Builder<K, V> readOnly(boolean readOnly) {
            this.readOnly = readOnly;
            return this;
        }
        public Builder<K, V> producerConfig(Map<String, Object> producerConfig) {
            this.producerConfig = producerConfig;
            return this;
        }
        public Builder<K, V> producerConfigBuilder(ProducerConfigBuilder producerConfigBuilder) {
            this.producerConfigBuilder = producerConfigBuilder;
            return this;
        }
        public Builder<K, V> storeData(boolean storeData) {
            this.storeData = storeData;
            return this;
        }
        public Builder<K, V> fireUpdateModeEffective() {
            return fireUpdateMode(FireUpdateMode.EFFECTIVE);
        }
        public Builder<K, V> fireUpdateModeAll() {
            return fireUpdateMode(FireUpdateMode.ALL);
        }
        public Builder<K, V> fireUpdateMode(FireUpdateMode fireUpdateMode) {
            this.fireUpdateMode = fireUpdateMode;
            return this;
        }
        public Builder<K, V> listener(CacheListener<K, V> listener) {
            this.listener = listener;
            return this;
        }

        public KafkaCache<K, V> build() {
            return new KafkaCache<>(
                    bootstrapServers,
                    topicName,
                    topicPartitionCount,
                    topicReplicationFactor,
                    topicConfigBuilder != null ? topicConfigBuilder.build() : topicConfig,
                    bootstrapTimeoutInMs,
                    consumerConfigBuilder != null ? consumerConfigBuilder.build() : consumerConfig,
                    offsetInitializer,
                    keyValueDecoder,
                    consumerParallelism,
                    readOnly,
                    producerConfigBuilder != null ? producerConfigBuilder.build() : producerConfig,
                    storeData,
                    fireUpdateMode,
                    listener);
        }

    }

}
