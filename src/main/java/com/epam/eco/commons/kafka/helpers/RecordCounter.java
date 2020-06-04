/*
 * Copyright 2020 EPAM Systems
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
package com.epam.eco.commons.kafka.helpers;

import java.util.Map;

import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;

import com.epam.eco.commons.kafka.AdminClientUtils;
import com.epam.eco.commons.kafka.OffsetRange;
import com.epam.eco.commons.kafka.config.ConsumerConfigBuilder;
import com.epam.eco.commons.kafka.consumer.bootstrap.BootstrapConsumer;
import com.epam.eco.commons.kafka.consumer.bootstrap.CountingRecordsCollector;

/**
 * @author Andrei_Tytsik
 */
public class RecordCounter {

    private static final long DEFAULT_BOOTSTRAP_TIMEOUT = 10 * 60 * 10000;

    private final Map<String, Object> consumerConfig;
    private final TopicOffsetRangeFetcher offsetRangeFetcher;

    private RecordCounter(String bootstrapServers, Map<String, Object> consumerConfig) {
        this.consumerConfig = ConsumerConfigBuilder.
                with(consumerConfig).
                bootstrapServers(bootstrapServers).
                minRequiredConfigs().
                enableAutoCommitDisabled().
                autoOffsetResetEarliest().
                clientIdRandom().
                build();
        this.offsetRangeFetcher = TopicOffsetRangeFetcher.with(consumerConfig);
    }

    public static RecordCounter with(Map<String, Object> consumerConfig) {
        return new RecordCounter(null, consumerConfig);
    }

    public static RecordCounter with(String bootstrapServers) {
        return new RecordCounter(bootstrapServers, null);
    }

    public long count(String topicName) {
        Validate.notBlank(topicName, "Topic name is blank");

        if (isTopicLogCompacted(topicName)) {
            return countByBootstrapingData(topicName);
        } else {
            return countByComparingOffsets(topicName);
        }
    }

    private boolean isTopicLogCompacted(String topicName) {
        ConfigEntry configEntry = AdminClientUtils.describeTopicConfigEntry(
                consumerConfig,
                topicName,
                TopicConfig.CLEANUP_POLICY_CONFIG);
        return TopicConfig.CLEANUP_POLICY_COMPACT.equals(configEntry.value());
    }

    private long countByComparingOffsets(String topicName) {
        Map<TopicPartition, OffsetRange> topicOffsets = fetchTopicOffsets(topicName);
        long count = 0;
        for (OffsetRange range : topicOffsets.values()) {
            count += range.getSize();
        }
        return count;
    }

    private long countByBootstrapingData(String topicName) {
        try (BootstrapConsumer<Object, Object, Long> bootstrapConsumer =
                BootstrapConsumer.<Object, Object, Long>builder().
                    topicName(topicName).
                    consumerConfig(consumerConfig).
                    bootstrapTimeoutInMs(DEFAULT_BOOTSTRAP_TIMEOUT).
                    recordCollector(new CountingRecordsCollector<>()).
                    build()) {
            return bootstrapConsumer.fetch();
        }
    }

    private Map<TopicPartition, OffsetRange> fetchTopicOffsets(String topicName) {
        return offsetRangeFetcher.fetchForTopics(topicName);
    }

}
