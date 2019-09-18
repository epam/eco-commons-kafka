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
package com.epam.eco.commons.kafka.helpers;

import java.util.Map;
import java.util.function.Consumer;

import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.TopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.epam.eco.commons.kafka.AdminClientUtils;
import com.epam.eco.commons.kafka.config.ConsumerConfigBuilder;
import com.epam.eco.commons.kafka.config.TopicConfigBuilder;

import kafka.server.KafkaConfig;

/**
 * @author Andrei_Tytsik
 */
public class TopicPurger {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopicPurger.class);

    private static final Map<String, String> PURGING_CONFIG = TopicConfigBuilder.withEmpty().
            cleanupPolicyDelete().
            retentionMs(10).
            buildStringified();

    private final Map<String, Object> consumerConfig;
    private final RecordCounter recordCounter;

    private TopicPurger(String bootstrapServers, Map<String, Object> consumerConfig) {
        this.consumerConfig = ConsumerConfigBuilder.
                with(consumerConfig).
                bootstrapServers(bootstrapServers).
                build();
        recordCounter = RecordCounter.with(consumerConfig);
    }

    public static TopicPurger with(Map<String, Object> consumerConfig) {
        return new TopicPurger(null, consumerConfig);
    }

    public static TopicPurger with(String bootstrapServers) {
        return new TopicPurger(bootstrapServers, null);
    }

    public void purge(String topicName) {
        purge(topicName, null);
    }

    public void purge(String topicName, Consumer<Map<String, String>> topicConfigBackup) {
        Validate.notBlank(topicName, "Topic name is blank");

        AdminClientUtils.callUnchecked(consumerConfig, adminClient -> {
            long logCleanerCheckIntervalMs = getLogCleanerCheckIntervalMs(
                    adminClient,
                    topicName);

            Config topicConfig = AdminClientUtils.describeTopicConfig(
                    adminClient,
                    topicName);
            if (!canBePurged(topicConfig, logCleanerCheckIntervalMs)) {
                LOGGER.warn(
                        "Topic '{}' can't be purged as it's configured to have very short retention period",
                        topicName);
                return null;
            }

            Map<String, String> topicConfigMap = AdminClientUtils.configToMap(topicConfig);
            if (topicConfigBackup != null) {
                topicConfigBackup.accept(topicConfigMap);
            }

            AdminClientUtils.alterTopicConfig(adminClient, topicName, PURGING_CONFIG);

            long delayBetweenChecks = logCleanerCheckIntervalMs / 10;

            long start = System.currentTimeMillis();
            long count = 0;
            do {
                try {
                    Thread.sleep(delayBetweenChecks);
                } catch (InterruptedException ie) {
                    // ignore
                }
                count = recordCounter.count(topicName);
            } while (count > 0 && System.currentTimeMillis() - start <= logCleanerCheckIntervalMs);

            doRestore(adminClient, topicName, topicConfigMap);

            return null;
        });
    }

    public void restore(
            String topicName,
            Map<String, String> topicConfig) {
        Validate.notBlank(topicName, "Topic name is blank");
        Validate.notNull(topicConfig, "Topic config is null");

        AdminClientUtils.callUnchecked(consumerConfig, adminClient -> {
            doRestore(
                    adminClient,
                    topicName,
                    topicConfig);
            return null;
        });
    }

    private void doRestore(
            AdminClient adminClient,
            String topicName,
            Map<String, String> topicConfig) {
        AdminClientUtils.alterTopicConfig(adminClient, topicName, topicConfig);
    }

    private boolean canBePurged(Config topicConfig, long logCleanerCheckIntervalMs) {
        String cleanupPolicy = topicConfig.get(TopicConfig.CLEANUP_POLICY_CONFIG).value();
        long retentionMs = Long.parseLong(topicConfig.get(TopicConfig.RETENTION_MS_CONFIG).value());
        return
                TopicConfig.CLEANUP_POLICY_COMPACT.equals(cleanupPolicy) ||
                retentionMs > logCleanerCheckIntervalMs;
    }

    private long getLogCleanerCheckIntervalMs(AdminClient adminClient, String topicName) {
        TopicDescription topicDescription = AdminClientUtils.describeTopic(
                adminClient,
                topicName);
        int brokerId = topicDescription.partitions().get(0).leader().id();
        return Long.parseLong(
                AdminClientUtils.describeBrokerConfigEntry(
                        adminClient,
                        brokerId,
                        KafkaConfig.LogCleanupIntervalMsProp()).value());
    }

}
