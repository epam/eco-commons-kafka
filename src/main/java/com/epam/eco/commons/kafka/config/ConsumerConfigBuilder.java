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
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.epam.eco.commons.kafka.OffsetReset;

/**
 * @author Andrei_Tytsik
 */
public class ConsumerConfigBuilder extends AbstractClientConfigBuilder<ConsumerConfigBuilder> {

    public static final String AUTH_EXCEPTION_RETRY_INTERVAL_MS = "authExceptionRetryIntervalMs";

    private ConsumerConfigBuilder(Map<String, Object> properties) {
        super(properties);
    }

    public static ConsumerConfigBuilder with(Map<String, Object> properties) {
        return new ConsumerConfigBuilder(properties);
    }

    public static ConsumerConfigBuilder withEmpty() {
        return new ConsumerConfigBuilder(null);
    }

    public ConsumerConfigBuilder minRequiredConfigs() {
        return
                groupIdRandomIfAbsent().
                keyDeserializerByteArrayIfAbsent().
                valueDeserializerByteArrayIfAbsent();
    }

    public ConsumerConfigBuilder maxPollRecordsMin() {
        return maxPollRecords(1);
    }

    public ConsumerConfigBuilder maxPollRecordsMax() {
        return maxPollRecords(Integer.MAX_VALUE);
    }

    public ConsumerConfigBuilder maxPollRecords(int maxPollRecords) {
        return property(
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
                maxPollRecords);
    }

    public ConsumerConfigBuilder enableAutoCommitDisabled() {
        return enableAutoCommit(false);
    }

    public ConsumerConfigBuilder enableAutoCommitEnabled() {
        return enableAutoCommit(true);
    }

    public ConsumerConfigBuilder enableAutoCommit(boolean enableAutoCommit) {
        return property(
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                enableAutoCommit);
    }

    public ConsumerConfigBuilder autoCommitIntervalMs(int autoCommitIntervalMs) {
        return property(
                ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,
                autoCommitIntervalMs);
    }

    public <A extends ConsumerPartitionAssignor> ConsumerConfigBuilder partitionAssignmentStrategy(
            Class<A> partitionAssignmentStrategy) {
        return partitionAssignmentStrategy(partitionAssignmentStrategy.getName());
    }

    public <A extends ConsumerPartitionAssignor> ConsumerConfigBuilder partitionAssignmentStrategy(
            List<Class<? extends A>> partitionAssignmentStrategy) {
        return partitionAssignmentStrategy(
                partitionAssignmentStrategy.stream().map(Class::getName).collect(Collectors.joining(",")));
    }

    public ConsumerConfigBuilder partitionAssignmentStrategy(String partitionAssignmentStrategy) {
        return property(
                ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                partitionAssignmentStrategy);
    }

    public ConsumerConfigBuilder autoOffsetResetEarliest() {
        return autoOffsetReset(OffsetReset.EARLIEST.name);
    }

    public ConsumerConfigBuilder autoOffsetResetLatest() {
        return autoOffsetReset(OffsetReset.LATEST.name);
    }

    public ConsumerConfigBuilder autoOffsetResetNone() {
        return autoOffsetReset(OffsetReset.NONE.name);
    }

    public ConsumerConfigBuilder autoOffsetReset(OffsetReset autoOffsetReset) {
        return autoOffsetReset(autoOffsetReset.name);
    }

    public ConsumerConfigBuilder autoOffsetReset(String autoOffsetReset) {
        return property(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                autoOffsetReset);
    }

    public ConsumerConfigBuilder fetchMinBytes(int fetchMinBytes) {
        return property(
                ConsumerConfig.FETCH_MIN_BYTES_CONFIG,
                fetchMinBytes);
    }

    public ConsumerConfigBuilder fetchMaxBytesDefault() {
        return fetchMaxBytes(ConsumerConfig.DEFAULT_FETCH_MAX_BYTES);
    }

    public ConsumerConfigBuilder fetchMaxBytes(int fetchMaxBytes) {
        return property(
                ConsumerConfig.FETCH_MAX_BYTES_CONFIG,
                fetchMaxBytes);
    }

    public ConsumerConfigBuilder fetchMaxWaitMs(int fetchMaxWaitMs) {
        return property(
                ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG,
                fetchMaxWaitMs);
    }

    public ConsumerConfigBuilder maxPartitionFetchBytesDefault() {
        return maxPartitionFetchBytes(ConsumerConfig.DEFAULT_MAX_PARTITION_FETCH_BYTES);
    }

    public ConsumerConfigBuilder maxPartitionFetchBytes(int maxPartitionFetchBytes) {
        return property(
                ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,
                maxPartitionFetchBytes);
    }

    public ConsumerConfigBuilder checkCrcsEnabled() {
        return checkCrcs(true);
    }

    public ConsumerConfigBuilder checkCrcsDisabled() {
        return checkCrcs(false);
    }

    public ConsumerConfigBuilder checkCrcs(boolean checkCrcs) {
        return property(
                ConsumerConfig.CHECK_CRCS_CONFIG,
                checkCrcs);
    }

    public ConsumerConfigBuilder keyDeserializerByteArray() {
        return keyDeserializer(ByteArrayDeserializer.class);
    }

    public ConsumerConfigBuilder keyDeserializerByteArrayIfAbsent() {
        return propertyIfAbsent(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class);
    }

    public ConsumerConfigBuilder keyDeserializerString() {
        return keyDeserializer(StringDeserializer.class);
    }

    public <D extends Deserializer<?>> ConsumerConfigBuilder keyDeserializer(
            Class<D> deserializerClass) {
        return property(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializerClass);
    }

    public ConsumerConfigBuilder valueDeserializerByteArray() {
        return valueDeserializer(ByteArrayDeserializer.class);
    }

    public ConsumerConfigBuilder valueDeserializerByteArrayIfAbsent() {
        return propertyIfAbsent(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class);
    }

    public ConsumerConfigBuilder valueDeserializerString() {
        return valueDeserializer(StringDeserializer.class);
    }

    public <D extends Deserializer<?>> ConsumerConfigBuilder valueDeserializer(
            Class<D> deserializerClass) {
        return property(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                deserializerClass);
    }

    public <D extends Deserializer<?>> ConsumerConfigBuilder deserializer(
            Class<D> deserializerClass,
            boolean isKey) {
        if (isKey) {
            return keyDeserializer(deserializerClass);
        } else {
            return valueDeserializer(deserializerClass);
        }
    }

    public <I extends ConsumerInterceptor<?, ?>> ConsumerConfigBuilder interceptorClasses(
            Class<I> interceptorClass) {
        return interceptorClasses(interceptorClass.getName());
    }

    public <I extends ConsumerInterceptor<?, ?>> ConsumerConfigBuilder interceptorClasses(
            List<Class<? extends I>> interceptorClasses) {
        return interceptorClasses(
                interceptorClasses.stream().map(Class::getName).collect(Collectors.joining(",")));
    }

    public ConsumerConfigBuilder interceptorClasses(String interceptorClasses) {
        return property(
                ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
                interceptorClasses);
    }

    public ConsumerConfigBuilder excludeInternalTopicsDefault() {
        return excludeInternalTopics(ConsumerConfig.DEFAULT_EXCLUDE_INTERNAL_TOPICS);
    }

    public ConsumerConfigBuilder excludeInternalTopicsEnabled() {
        return excludeInternalTopics(true);
    }

    public ConsumerConfigBuilder excludeInternalTopicsDisabled() {
        return excludeInternalTopics(false);
    }

    public ConsumerConfigBuilder excludeInternalTopics(boolean excludeInternalTopics) {
        return property(
                ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG,
                excludeInternalTopics);
    }

    public ConsumerConfigBuilder isolationLevelDefault() {
        return isolationLevel(ConsumerConfig.DEFAULT_ISOLATION_LEVEL);
    }

    public ConsumerConfigBuilder isolationLevelReadUncommited() {
        return isolationLevel(IsolationLevel.READ_UNCOMMITTED);
    }

    public ConsumerConfigBuilder isolationLevelReadCommited() {
        return isolationLevel(IsolationLevel.READ_COMMITTED);
    }

    public ConsumerConfigBuilder isolationLevel(IsolationLevel isolationLevel) {
        return isolationLevel(isolationLevel.toString().toLowerCase(Locale.ROOT));
    }

    public ConsumerConfigBuilder isolationLevel(String isolationLevel) {
        return property(
                ConsumerConfig.ISOLATION_LEVEL_CONFIG,
                isolationLevel);
    }

    public ConsumerConfigBuilder allowAutoCreateTopicDefault() {
        return allowAutoCreateTopic(ConsumerConfig.DEFAULT_ALLOW_AUTO_CREATE_TOPICS);
    }

    public ConsumerConfigBuilder allowAutoCreateTopicEnabled() {
        return allowAutoCreateTopic(true);
    }

    public ConsumerConfigBuilder allowAutoCreateTopicDisabled() {
        return allowAutoCreateTopic(false);
    }

    public ConsumerConfigBuilder allowAutoCreateTopic(boolean allowAutoCreateTopic) {
        return property(
                ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG,
                allowAutoCreateTopic);
    }

    /**
     * Set the interval between retries after {@link org.apache.kafka.common.errors.AuthorizationException AuthorizationException} is thrown by consumer.
     * By default, the field is null and retries are disabled,
     * in such case {@link org.apache.kafka.common.errors.AuthorizationException AuthorizationException} is treated as fatal and the consumer is stopped.
     *
     * @param authExceptionRetryIntervalMs the duration between retries
     */
    public ConsumerConfigBuilder authExceptionRetryIntervalMs(long authExceptionRetryIntervalMs) {
        return property(
                AUTH_EXCEPTION_RETRY_INTERVAL_MS,
                authExceptionRetryIntervalMs);
    }

}
