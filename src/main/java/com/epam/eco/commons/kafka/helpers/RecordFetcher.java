/*******************************************************************************
 *  Copyright 2023 EPAM Systems
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
package com.epam.eco.commons.kafka.helpers;

import java.util.Collection;
import java.util.Map;
import java.util.function.Predicate;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

public interface RecordFetcher<K,V> {
    @Deprecated
    RecordFetchResult<K, V> fetch(
            String[] topicNames,
            long offset,
            long limit,
            long timeoutInMs);
    @Deprecated
    RecordFetchResult<K, V> fetch(
            Collection<String> topicNames,
            long offset,
            long limit,
            long timeoutInMs);
    @Deprecated
    RecordFetchResult<K, V> fetch(
            Collection<String> topicNames,
            long offset,
            long limit,
            Predicate<ConsumerRecord<K, V>> filter,
            long timeoutInMs);
    @Deprecated
    RecordFetchResult<K, V> fetch(
            String[] topicNames,
            long offset,
            long limit,
            Predicate<ConsumerRecord<K, V>> filter,
            long timeoutInMs);
    @Deprecated
    RecordFetchResult<K, V> fetch(
            Map<TopicPartition, Long> offsets,
            long limit,
            long timeoutInMs);
    @Deprecated
    RecordFetchResult<K, V> fetch(
            Map<TopicPartition, Long> offsets,
            long limit,
            Predicate<ConsumerRecord<K, V>> filter,
            long timeoutInMs);



    RecordFetchResult<K, V> fetchByOffsets(
            String[] topicNames,
            long offset,
            long limit,
            long timeoutInMs);
    RecordFetchResult<K, V> fetchByOffsets(
            Collection<String> topicNames,
            long offset,
            long limit,
            long timeoutInMs);
    RecordFetchResult<K, V> fetchByOffsets(
            Collection<String> topicNames,
            long offset,
            long limit,
            Predicate<ConsumerRecord<K, V>> filter,
            long timeoutInMs);
    RecordFetchResult<K, V> fetchByOffsets(
            String[] topicNames,
            long offset,
            long limit,
            Predicate<ConsumerRecord<K, V>> filter,
            long timeoutInMs);
    RecordFetchResult<K, V> fetchByOffsets(
            Map<TopicPartition, Long> offsets,
            long limit,
            long timeoutInMs);
    RecordFetchResult<K, V> fetchByOffsets(
            Map<TopicPartition, Long> offsets,
            long limit,
            Predicate<ConsumerRecord<K, V>> filter,
            long timeoutInMs);

    RecordFetchResult<K, V> fetchByTimestamps(
            Map<TopicPartition, Long> partitionTimestamps,
            long limit,
            long timeoutInMs);
    RecordFetchResult<K, V> fetchByTimestamps(
            Map<TopicPartition, Long> partitionTimestamps,
            long limit,
            Predicate<ConsumerRecord<K, V>> filter,
            long timeoutInMs);
}
