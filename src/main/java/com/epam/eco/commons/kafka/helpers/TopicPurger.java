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

import org.apache.commons.lang3.Validate;

import com.epam.eco.commons.kafka.AdminClientUtils;
import com.epam.eco.commons.kafka.config.ConsumerConfigBuilder;

/**
 * @author Andrei_Tytsik
 */
public class TopicPurger {

    private final Map<String, Object> consumerConfig;

    private TopicPurger(String bootstrapServers, Map<String, Object> consumerConfig) {
        this.consumerConfig = ConsumerConfigBuilder.
                with(consumerConfig).
                bootstrapServers(bootstrapServers).
                build();
    }

    public static TopicPurger with(Map<String, Object> consumerConfig) {
        return new TopicPurger(null, consumerConfig);
    }

    public static TopicPurger with(String bootstrapServers) {
        return new TopicPurger(bootstrapServers, null);
    }

    public void purge(String topicName) {
        Validate.notBlank(topicName, "Topic name is blank");

        AdminClientUtils.deleteAllRecords(consumerConfig, topicName);
    }

}