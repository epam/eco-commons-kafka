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

import java.util.Map;
import java.util.function.Predicate;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import com.epam.eco.commons.kafka.helpers.BiDirectionalTopicRecordFetcher.FetchDirection;

public interface RecordBiDirectionalFetcher<K,V> {
    RecordFetchResult<K, V> fetchByOffsets(
            Map<TopicPartition, Long> offsets,
            long limit,
            FilterClausePredicate<K,V> filter,
            long timeoutInMs,
            FetchDirection direction );

    RecordFetchResult<K, V> fetchByTimestamps(
            Map<TopicPartition, Long> timestamps,
            long limit,
            FilterClausePredicate<K,V> filter,
            long timeoutInMs,
            FetchDirection direction );

}