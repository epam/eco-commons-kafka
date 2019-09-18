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

import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.epam.eco.commons.kafka.AdminClientUtils;
import com.epam.eco.commons.kafka.config.AdminClientConfigBuilder;
import com.epam.eco.commons.kafka.config.TopicConfigBuilder;

import kafka.server.KafkaConfig;

/**
 * @author Andrei_Tytsik
 */
class CacheTopic {

    private final static Logger LOGGER = LoggerFactory.getLogger(CacheTopic.class);

    private final String name;
    private final int partitionCount;
    private final short replicationFactor;
    private final Map<String, Object> config;
    private final Map<String, Object> clientConfig;

    public CacheTopic(
            String bootstrapServers,
            String name,
            int partitionCount,
            int replicationFactor,
            Map<String, Object> config,
            Map<String, Object> clientConfig) {
        this.name = name;
        this.partitionCount = partitionCount;
        this.replicationFactor = (short)replicationFactor;
        this.config = config;
        this.clientConfig = AdminClientConfigBuilder.with(clientConfig).
                bootstrapServers(bootstrapServers).
                build();
    }

    public void createIfNotExists() {
        AdminClientUtils.callUnchecked(clientConfig, client -> {
            if (AdminClientUtils.topicExists(client, name)) {
                LOGGER.info("Cache topic [{}] exists", name);
            } else {
                LOGGER.info("Cache topic [{}] doesn't exist, creating it", name);

                AdminClientUtils.createTopic(client, newTopic());
            }
            return null;
        });
    }

    private NewTopic newTopic() {
        int partitionCount = this.partitionCount > 0 ? this.partitionCount : 1;
        short replicationFactor =
                this.replicationFactor > 0 ?
                this.replicationFactor :
                Short.parseShort(
                        AdminClientUtils.describeAnyBrokerConfigEntry(
                                clientConfig,
                                KafkaConfig.DefaultReplicationFactorProp()).value());
        Map<String, String> config = TopicConfigBuilder.with(this.config).
                cleanupPolicyCompact().
                buildStringified();

        NewTopic newTopic = new NewTopic(name, partitionCount, replicationFactor);
        newTopic.configs(config);
        return newTopic;
    }

}
