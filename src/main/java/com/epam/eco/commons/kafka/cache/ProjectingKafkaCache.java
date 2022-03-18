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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.epam.eco.commons.kafka.KafkaUtils;
import com.epam.eco.commons.kafka.config.ConsumerConfigBuilder;
import com.epam.eco.commons.kafka.consumer.bootstrap.OffsetInitializer;
import com.epam.eco.commons.kafka.serde.KeyValueDecoder;

/**
 * @author Andrei_Tytsik
 */
public class ProjectingKafkaCache<K, V, P> extends AbstractCache<K, P> {

    private final static Logger LOGGER = LoggerFactory.getLogger(ProjectingKafkaCache.class);

    private final ListCacheConsumer<K, V> consumer;
    private final ProjectionBuilder<K, V, P> projectionBuilder;

    private ProjectingKafkaCache(
            String bootstrapServers,
            String topicName,
            long bootstrapTimeoutInMs,
            Map<String, Object> consumerConfig,
            OffsetInitializer offsetInitializer,
            KeyValueDecoder<K, V> keyValueDecoder,
            int consumerParallelism,
            ProjectionBuilder<K, V, P> projectionBuilder,
            FireUpdateMode fireUpdateMode,
            CacheListener<K, P> listener) {
        super(true, fireUpdateMode, listener);

        Validate.notBlank(bootstrapServers, "Bootstrap servers list is blank");
        Validate.notBlank(topicName, "Topic name is blank");
        Validate.notNull(projectionBuilder, "Projection builder is null");

        consumer = new ListCacheConsumer<>(
                topicName,
                bootstrapServers,
                bootstrapTimeoutInMs,
                consumerConfig,
                offsetInitializer,
                keyValueDecoder,
                consumerParallelism,
                update -> applyUpdateFromUnderlying(project(update)));

        this.projectionBuilder = projectionBuilder;

        LOGGER.info("Initialized");
    }

    @Override
    public void start() throws Exception {
        consumer.start();

        LOGGER.info("Started");
    }

    @Override
    public void close() {
        KafkaUtils.closeQuietly(consumer);

        LOGGER.info("Closed");
    }

    private Map<K, P> project(List<ConsumerRecord<K, V>> update) {
        if (update.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<K, List<V>> logs = new HashMap<>();
        for (ConsumerRecord<K, V> record : update) {
            K key = record.key();
            V value = record.value();

            List<V> log = logs.get(key);
            if (log == null) {
                log = new ArrayList<>();
                logs.put(key, log);
            }

            log.add(value);
        }

        Map<K, P> projections = new HashMap<>(logs.size(), 1);
        for (Entry<K, List<V>> entry : logs.entrySet()) {
            K key = entry.getKey();
            List<V> log = entry.getValue();
            P previous = get(key);
            P projection = projectionBuilder.build(previous, key, log);
            projections.put(key, projection);
        }

        return projections;
    }

    public static <K, V, P> Builder<K, V, P> builder() {
        return new Builder<>();
    }

    public static class Builder<K, V, P> {

        private String bootstrapServers;
        private String topicName;
        private long bootstrapTimeoutInMs = 30000;
        private Map<String, Object> consumerConfig;
        private ConsumerConfigBuilder consumerConfigBuilder;
        private OffsetInitializer offsetInitializer;
        private KeyValueDecoder<K, V> keyValueDecoder;
        private int consumerParallelism = 1;
        private ProjectionBuilder<K, V, P> projectionBuilder;
        private FireUpdateMode fireUpdateMode = FireUpdateMode.EFFECTIVE;
        private CacheListener<K, P> listener;

        public Builder<K, V, P> bootstrapServers(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
            return this;
        }
        public Builder<K, V, P> topicName(String topicName) {
            this.topicName = topicName;
            return this;
        }
        public Builder<K, V, P> bootstrapTimeoutInMs(long bootstrapTimeoutInMs) {
            this.bootstrapTimeoutInMs = bootstrapTimeoutInMs;
            return this;
        }
        public Builder<K, V, P> consumerConfig(Map<String, Object> consumerConfig) {
            this.consumerConfig = consumerConfig;
            return this;
        }
        public Builder<K, V, P> consumerConfigBuilder(ConsumerConfigBuilder consumerConfigBuilder) {
            this.consumerConfigBuilder = consumerConfigBuilder;
            return this;
        }
        public Builder<K, V, P> offsetInitializer(OffsetInitializer offsetInitializer) {
            this.offsetInitializer = offsetInitializer;
            return this;
        }
        public Builder<K, V, P> keyValueDecoder(KeyValueDecoder<K, V> keyValueDecoder) {
            this.keyValueDecoder = keyValueDecoder;
            return this;
        }
        public Builder<K, V, P> consumerParallelism(int consumerParallelism) {
            this.consumerParallelism = consumerParallelism;
            return this;
        }
        public Builder<K, V, P> consumerParallelismAvailableCores() {
            this.consumerParallelism = Runtime.getRuntime().availableProcessors();
            return this;
        }
        public Builder<K, V, P> projectionBuilder(ProjectionBuilder<K, V, P> projectionBuilder) {
            this.projectionBuilder = projectionBuilder;
            return this;
        }
        public Builder<K, V, P> fireUpdateModeEffective() {
            return fireUpdateMode(FireUpdateMode.EFFECTIVE);
        }
        public Builder<K, V, P> fireUpdateModeAll() {
            return fireUpdateMode(FireUpdateMode.ALL);
        }
        public Builder<K, V, P> fireUpdateMode(FireUpdateMode fireUpdateMode) {
            this.fireUpdateMode = fireUpdateMode;
            return this;
        }
        public Builder<K, V, P> listener(CacheListener<K, P> listener) {
            this.listener = listener;
            return this;
        }

        public ProjectingKafkaCache<K, V, P> build() {
            return new ProjectingKafkaCache<>(
                    bootstrapServers,
                    topicName,
                    bootstrapTimeoutInMs,
                    consumerConfigBuilder != null ? consumerConfigBuilder.build() : consumerConfig,
                    offsetInitializer,
                    keyValueDecoder,
                    consumerParallelism,
                    projectionBuilder,
                    fireUpdateMode,
                    listener);
        }

    }

}
