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
package com.epam.eco.commons.kafka.config;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.epam.eco.commons.kafka.Acks;


/**
 * @author Andrei_Tytsik
 */
public class ProducerConfigBuilder extends AbstractClientConfigBuilder<ProducerConfigBuilder> {

    private ProducerConfigBuilder(Map<String, Object> properties) {
        super(properties);
    }

    public static ProducerConfigBuilder with(Map<String, Object> properties) {
        return new ProducerConfigBuilder(properties);
    }

    public static ProducerConfigBuilder withEmpty() {
        return new ProducerConfigBuilder(null);
    }

    public ProducerConfigBuilder minRequiredConfigs() {
        return
                keySerializerByteArrayIfAbsent().
                valueSerializerByteArrayIfAbsent();
    }

    public ProducerConfigBuilder metadataMaxIdle(long metadataMaxIdle) {
        return property(ProducerConfig.METADATA_MAX_IDLE_CONFIG, metadataMaxIdle);
    }

    public ProducerConfigBuilder batchSize(int batchSize) {
        return property(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
    }

    public ProducerConfigBuilder acksAll() {
        return acks(Acks.ALL);
    }

    public ProducerConfigBuilder acksNone() {
        return acks(Acks.NONE);
    }

    public ProducerConfigBuilder acksOne() {
        return acks(Acks.ONE);
    }

    public ProducerConfigBuilder acks(Acks acks) {
        return acks(acks.name);
    }

    public ProducerConfigBuilder acks(String acks) {
        return property(ProducerConfig.ACKS_CONFIG, acks);
    }

    public ProducerConfigBuilder lingerMs(int lingerMs) {
        return property(ProducerConfig.LINGER_MS_CONFIG, lingerMs);
    }

    public ProducerConfigBuilder deliveryTimeoutMs(int deliveryTimeoutMs) {
        return property(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, deliveryTimeoutMs);
    }

    public ProducerConfigBuilder maxRequestSize(int maxRequestSize) {
        return property(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, maxRequestSize);
    }

    public ProducerConfigBuilder maxBlockMsMax() {
        return maxBlockMs(Long.MAX_VALUE);
    }

    public ProducerConfigBuilder maxBlockMs(long maxBlockMs) {
        return property(ProducerConfig.MAX_BLOCK_MS_CONFIG, maxBlockMs);
    }

    public ProducerConfigBuilder bufferMemory(long bufferMemory) {
        return property(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemory);
    }

    public ProducerConfigBuilder compressionType(CompressionType compressionType) {
        return compressionType(compressionType.name);
    }

    public ProducerConfigBuilder compressionType(String compressionType) {
        return property(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);
    }

    public ProducerConfigBuilder maxInflightRequestsPerConnectionMin() {
        return maxInflightRequestsPerConnection(1);
    }

    public ProducerConfigBuilder maxInflightRequestsPerConnection(int maxRequests) {
        return property(
                ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
                maxRequests);
    }

    public ProducerConfigBuilder keySerializerByteArray() {
        return property(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                ByteArraySerializer.class);
    }

    public ProducerConfigBuilder keySerializerByteArrayIfAbsent() {
        return propertyIfAbsent(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                ByteArraySerializer.class);
    }

    public ProducerConfigBuilder keySerializerString() {
        return property(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class);
    }

    public <S extends Serializer<?>> ProducerConfigBuilder keySerializer(
            Class<S> serializerClass) {
        return property(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, serializerClass);
    }

    public ProducerConfigBuilder valueSerializerByteArray() {
        return property(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                ByteArraySerializer.class);
    }

    public ProducerConfigBuilder valueSerializerByteArrayIfAbsent() {
        return propertyIfAbsent(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                ByteArraySerializer.class);
    }

    public ProducerConfigBuilder valueSerializerString() {
        return property(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class);
    }

    public <S extends Serializer<?>> ProducerConfigBuilder valueSerializer(
            Class<S> serializerClass) {
        return property(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                serializerClass);
    }

    public <S extends Serializer<?>> ProducerConfigBuilder serializer(
            Class<S> serializerClass, boolean isKey) {
        if (isKey) {
            return keySerializer(serializerClass);
        } else {
            return valueSerializer(serializerClass);
        }
    }

    public <P extends Partitioner> ProducerConfigBuilder partitionerClass(Class<P> partitionerClass) {
        return property(ProducerConfig.PARTITIONER_CLASS_CONFIG, partitionerClass);
    }

    public <I extends ProducerInterceptor<?, ?>> ProducerConfigBuilder interceptorClasses(
            Class<I> interceptorClass) {
        return interceptorClasses(interceptorClass.getName());
    }

    public <I extends ProducerInterceptor<?, ?>> ProducerConfigBuilder interceptorClasses(
            List<Class<? extends I>> interceptorClasses) {
        return interceptorClasses(
                interceptorClasses.stream().map(Class::getName).collect(Collectors.joining(",")));
    }

    public ProducerConfigBuilder interceptorClasses(String interceptorClasses) {
        return property(
                ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                interceptorClasses);
    }

    public ProducerConfigBuilder enableIdempotenceEnabled() {
        return enableIdempotence(true);
    }

    public ProducerConfigBuilder enableIdempotenceDisabled() {
        return enableIdempotence(false);
    }

    public ProducerConfigBuilder enableIdempotence(boolean enableIdempotence) {
        return property(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, enableIdempotence);
    }

    public ProducerConfigBuilder transactionTimeout(int transactionTimeout) {
        return property(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, transactionTimeout);
    }

    public ProducerConfigBuilder transactionalId(String transactionalId) {
        return property(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
    }

}
