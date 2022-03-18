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
package com.epam.eco.commons.kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigDef.ConfigKey;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.epam.eco.commons.kafka.config.AbstractConfigDef;
import com.epam.eco.commons.kafka.config.BrokerConfigDef;
import com.epam.eco.commons.kafka.config.TopicConfigDef;


/**
 * @author Andrei_Tytsik
 */
public abstract class AdminClientUtils {

    public static final Config TOPIC_DEFAULT_CONFIG = createDefaultConfig(Type.TOPIC);
    public static final Config BROKER_DEFAULT_CONFIG = createDefaultConfig(Type.BROKER);

    private static final Logger LOGGER = LoggerFactory.getLogger(AdminClientUtils.class);

    private static final int PARALLELISM_THRESHOLD = 500;

    public interface AdminClientCallable<R> {
        R call(AdminClient client) throws Exception;
    }

    public interface UncheckedAdminClientCallable<R> {
        R call(AdminClient client);
    }

    private AdminClientUtils() {
    }

    public static void deleteTopic(Map<String, Object> clientConfig, String topicName) {
        try (AdminClient client = initClient(clientConfig)) {
            deleteTopic(client, topicName);
        }
    }

    public static void deleteTopic(AdminClient client, String topicName) {
        deleteTopics(client, Collections.singleton(topicName));
    }

    public static void deleteTopics(Map<String, Object> clientConfig, Collection<String> topicNames) {
        try (AdminClient client = initClient(clientConfig)) {
            deleteTopics(client, topicNames);
        }
    }

    public static void deleteTopics(AdminClient client, Collection<String> topicNames) {
        Validate.notNull(client, "Admin client is null");
        Validate.notEmpty(topicNames, "Collection of topic names is null or empty");
        Validate.noNullElements(topicNames, "Collection of topic names contains null elements");

        completeAndGet(
                client.deleteTopics(topicNames).all());
    }

    public static void createPartitions(
            Map<String, Object> clientConfig,
            String topicName,
            int newPartitionCount) {
        try (AdminClient client = initClient(clientConfig)) {
            createPartitions(client, topicName, newPartitionCount);
        }
    }

    public static void createPartitions(
            AdminClient client,
            String topicName,
            int newPartitionCount) {
        Validate.notNull(client, "Admin client is null");
        Validate.notBlank(topicName, "Topic name is blank");
        Validate.isTrue(newPartitionCount > 0, "Partition count is invalid");

        NewPartitions newPartitions = NewPartitions.increaseTo(newPartitionCount);
        completeAndGet(
                client.createPartitions(Collections.singletonMap(topicName, newPartitions)).all());
    }

    public static void createTopic(
            Map<String, Object> clientConfig,
            String topicName,
            int partitionCount,
            int replicationFactor,
            Map<String, String> config) {
        try (AdminClient client = initClient(clientConfig)) {
            createTopic(client, topicName, partitionCount, replicationFactor, config);
        }
    }

    public static void createTopic(
            AdminClient client,
            String topicName,
            int partitionCount,
            int replicationFactor,
            Map<String, String> config) {
        Validate.notNull(client, "Admin client is null");
        Validate.notBlank(topicName, "Topic name is blank");
        Validate.isTrue(partitionCount > 0, "Partition count is invalid");
        Validate.isTrue(replicationFactor > 0, "Replication factor is invalid");

        NewTopic newTopic = new NewTopic(topicName, partitionCount, (short)replicationFactor);
        if (config != null) {
            newTopic.configs(config);
        }

        completeAndGet(
                client.createTopics(Collections.singletonList(newTopic)).all());
    }

    public static void createTopic(Map<String, Object> clientConfig, NewTopic newTopic) {
        try (AdminClient client = initClient(clientConfig)) {
            createTopic(client, newTopic);
        }
    }

    public static void createTopic(AdminClient client, NewTopic newTopic) {
        Validate.notNull(client, "Admin client is null");
        Validate.notNull(newTopic, "NewTopic is null");

        completeAndGet(
                client.createTopics(Collections.singletonList(newTopic)).all());
    }

    /**
     * @deprecated use {@link #resetAllTopicConfigs(Map, String)}
     */
    @Deprecated
    public static void resetTopicConfig(Map<String, Object> clientConfig, String topicName) {
        resetAllTopicConfigs(clientConfig, topicName);
    }

    public static void resetAllTopicConfigs(Map<String, Object> clientConfig, String topicName) {
        try (AdminClient client = initClient(clientConfig)) {
            resetAllTopicConfigs(client, topicName);
        }
    }

    public static void resetAllTopicConfigs(AdminClient client, String topicName) {
        Validate.notBlank(topicName, "Topic name is blank");

        resetAllResourceConfigs(client, new ConfigResource(Type.TOPIC, topicName));
    }

    /**
     * @deprecated use {@link #alterTopicConfigs(Map, String, Map)}
     */
    @Deprecated
    public static void alterTopicConfig(
            Map<String, Object> clientConfig,
            String topicName,
            Map<String, String> configMap) {
        alterTopicConfigs(clientConfig, topicName, configMap);
    }

    public static void alterTopicConfigs(
            Map<String, Object> clientConfig,
            String topicName,
            Map<String, String> configs) {
        try (AdminClient client = initClient(clientConfig)) {
            alterTopicConfigs(client, topicName, configs);
        }
    }

    /**
     * @deprecated use {@link #alterTopicConfigs(AdminClient, String, Map)}
     */
    @Deprecated
    public static void alterTopicConfig(
            AdminClient client,
            String topicName,
            Map<String, String> configs) {
        alterTopicConfigs(client, topicName, configs);
    }

    public static void alterTopicConfigs(
            AdminClient client,
            String topicName,
            Map<String, String> configs) {
        Validate.notBlank(topicName, "Topic name is blank");

        alterResourceConfigs(client, new ConfigResource(Type.TOPIC, topicName), configs);
    }

    public static Collection<TopicListing> listTopics(Map<String, Object> clientConfig) {
        return listTopics(clientConfig, false);
    }

    public static Collection<TopicListing> listTopics(Map<String, Object> clientConfig, boolean listInternalTopics) {
        try (AdminClient client = initClient(clientConfig)) {
            return listTopics(client, listInternalTopics);
        }
    }

    public static Collection<TopicListing> listTopics(AdminClient client) {
        return listTopics(client, false);
    }

    public static Collection<TopicListing> listTopics(AdminClient client, boolean listInternalTopics) {
        Validate.notNull(client, "Admin client is null");

        ListTopicsOptions options = new ListTopicsOptions();
        options.listInternal(listInternalTopics);

        return completeAndGet(client.listTopics(options).listings());
    }

    public static boolean topicExists(Map<String, Object> clientConfig, String topicName) {
        try (AdminClient client = initClient(clientConfig)) {
            return topicExists(client, topicName);
        }
    }

    public static boolean topicExists(AdminClient client, String topicName) {
        try {
            return describeTopic(client, topicName) != null;
        } catch (UnknownTopicOrPartitionException utpe) {
            return false;
        }
    }

    public static TopicDescription describeTopic(Map<String, Object> clientConfig, String topicName) {
        try (AdminClient client = initClient(clientConfig)) {
            return describeTopic(client, topicName);
        }
    }

    public static TopicDescription describeTopic(AdminClient client, String topicName) {
        Validate.notNull(client, "Admin client is null");
        Validate.notBlank(topicName, "Topic name is blank");

        return describeTopics(client, Collections.singleton(topicName)).get(topicName);
    }

    public static Map<String, TopicDescription> describeTopics(
            Map<String, Object> clientConfig,
            Collection<String> topicNames) {
        try (AdminClient client = initClient(clientConfig)) {
            return describeTopics(client, topicNames);
        }
    }

    public static Map<String, TopicDescription> describeTopics(
            AdminClient client,
            Collection<String> topicNames) {
        Validate.notNull(client, "Admin client is null");
        Validate.notEmpty(topicNames, "Collection of topic names is null or empty");
        Validate.noNullElements(topicNames, "Collection of topic names contains null elements");

        return completeAndGet(client.describeTopics(topicNames).all());
    }

    public static Map<String, TopicDescription> describeTopicsInParallel(
            Map<String, Object> clientConfig,
            Collection<String> topicNames) {
        Validate.notNull(clientConfig, "Config is null");
        Validate.notEmpty(topicNames, "Collection of topic names is null or empty");
        Validate.noNullElements(topicNames, "Collection of topic names contains null elements");

        if (topicNames.size() <= PARALLELISM_THRESHOLD) {
            return describeTopics(clientConfig, topicNames);
        }

        return describeInParallel(
                clientConfig,
                "topic",
                topicNames,
                (client, names) -> client.describeTopics(names).values());
    }

    public static ConfigEntry describeTopicConfigEntry(
            Map<String, Object> clientConfig,
            String topicName,
            String configName) {
        return describeTopicConfig(clientConfig, topicName).get(configName);
    }

    public static Map<String, String> describeTopicConfigAsMap(
            Map<String, Object> clientConfig,
            String topicName) {
        return describeTopicConfigAsMap(clientConfig, topicName, true, true);
    }

    public static Map<String, String> describeTopicConfigAsMap(AdminClient client, String topicName) {
        return describeTopicConfigAsMap(client, topicName, true, true);
    }

    public static Map<String, String> describeTopicConfigAsMap(
            Map<String, Object> clientConfig,
            String topicName,
            boolean ignoreDefaultConfigs,
            boolean ignoreReadOnlyConfigs) {
        try (AdminClient client = initClient(clientConfig)) {
            return describeTopicConfigAsMap(
                    client,
                    topicName,
                    ignoreDefaultConfigs,
                    ignoreReadOnlyConfigs);
        }
    }

    public static Map<String, String> describeTopicConfigAsMap(
            AdminClient client,
            String topicName,
            boolean ignoreDefaultConfigs,
            boolean ignoreReadOnlyConfigs) {
        Config config = describeTopicConfig(client, topicName);
        return
                config != null ?
                configToMap(config, ignoreDefaultConfigs, ignoreReadOnlyConfigs) :
                null;
    }

    public static Config describeTopicConfig(
            Map<String, Object> clientConfig,
            String topicName) {
        try (AdminClient client = initClient(clientConfig)) {
            return describeTopicConfig(client, topicName);
        }
    }

    public static Config describeTopicConfig(AdminClient client, String topicName) {
        return describeTopicConfigs(
                client,
                Collections.singleton(topicName)).get(topicName);
    }

    public static Map<String, Config> describeTopicConfigs(
            Map<String, Object> clientConfig,
            Collection<String> topicNames) {
        try (AdminClient client = initClient(clientConfig)) {
            return describeTopicConfigs(client, topicNames);
        }
    }

    public static Map<String, Config> describeTopicConfigs(
            AdminClient client,
            Collection<String> topicNames) {
        Validate.notNull(client, "Admin client is null");
        Validate.notEmpty(topicNames, "Collection of topic names is null or empty");
        Validate.noNullElements(topicNames, "Collection of topic names contains null elements");

        Set<ConfigResource> resources = new HashSet<>();
        topicNames.forEach(
                topicName -> resources.add(new ConfigResource(Type.TOPIC, topicName)));

        return describeConfigs(client, resources).entrySet().stream().
                collect(Collectors.toMap(
                        e -> e.getKey().name(),
                        Map.Entry::getValue));
    }

    public static ConfigEntry describeAnyBrokerConfigEntry(
            Map<String, Object> clientConfig,
            String configName) {
        try (AdminClient client = initClient(clientConfig)) {
            return describeAnyBrokerConfigEntry(client, configName);
        }
    }

    public static ConfigEntry describeAnyBrokerConfigEntry(
            AdminClient client,
            String configName) {
        Collection<Node> nodes = describeCluster(client);
        if (nodes == null || nodes.isEmpty()) {
            throw new RuntimeException("No brokers available");
        }

        return describeBrokerConfigEntry(client, nodes.iterator().next().id(), configName);
    }

    public static ConfigEntry describeBrokerConfigEntry(
            Map<String, Object> clientConfig,
            int brokerId,
            String configName) {
        return describeBrokerConfig(clientConfig, brokerId).get(configName);
    }

    public static ConfigEntry describeBrokerConfigEntry(
            AdminClient client,
            int brokerId,
            String configName) {
        return describeBrokerConfig(client, brokerId).get(configName);
    }

    public static Config describeBrokerConfig(Map<String, Object> clientConfig, int brokerId) {
        try (AdminClient client = initClient(clientConfig)) {
            return describeBrokerConfig(client, brokerId);
        }
    }

    public static Config describeBrokerConfig(AdminClient client, int brokerId) {
        return describeBrokerConfigs(
                client,
                Collections.singleton(brokerId)).get(brokerId);
    }

    public static Map<Integer, Config> describeBrokerConfigs(
            Map<String, Object> clientConfig,
            Collection<Integer> brokerIds) {
        try (AdminClient client = initClient(clientConfig)) {
            return describeBrokerConfigs(client, brokerIds);
        }
    }

    public static Map<Integer, Config> describeBrokerConfigs(
            AdminClient client,
            Collection<Integer> brokerIds) {
        Validate.notNull(client, "Admin client is null");
        Validate.notEmpty(brokerIds, "Collection of broker ids is null or empty");
        Validate.noNullElements(brokerIds, "Collection of broker ids contains null elements");

        Set<ConfigResource> resources = new HashSet<>();
        brokerIds.forEach(
                brokerId -> resources.add(new ConfigResource(Type.BROKER, "" + brokerId)));

        return describeConfigs(client, resources).entrySet().stream().
                collect(Collectors.toMap(
                        e -> Integer.valueOf(e.getKey().name()),
                        Map.Entry::getValue));
    }

    public static Collection<Node> describeCluster(Map<String, Object> clientConfig) {
        try (AdminClient client = initClient(clientConfig)) {
            return describeCluster(client);
        }
    }

    public static Collection<Node> describeCluster(AdminClient client) {
        Validate.notNull(client, "Admin client is null");

        return completeAndGet(
                client.describeCluster().nodes());
    }

    public static void resetAllBrokerConfigs(Map<String, Object> clientConfig, int brokerId) {
        try (AdminClient client = initClient(clientConfig)) {
            resetAllBrokerConfigs(client, brokerId);
        }
    }

    public static void resetAllBrokerConfigs(AdminClient client, int brokerId) {
        Validate.isTrue(brokerId >= 0, "Broker id is invalid: %d", brokerId);

        resetAllResourceConfigs(client, new ConfigResource(Type.BROKER, "" + brokerId));
    }

    public static void alterBrokerConfigs(
            Map<String, Object> clientConfig,
            int brokerId,
            Map<String, String> configs) {
        try (AdminClient client = initClient(clientConfig)) {
            alterBrokerConfigs(client, brokerId, configs);
        }
    }

    public static void alterBrokerConfigs(
            AdminClient client,
            int brokerId,
            Map<String, String> configs) {
        Validate.isTrue(brokerId >= 0, "Broker id is invalid: %d", brokerId);

        alterResourceConfigs(client, new ConfigResource(Type.BROKER, "" + brokerId), configs);
    }

    public static void createAcl(Map<String, Object> clientConfig, AclBinding aclBinding) {
        try (AdminClient client = initClient(clientConfig)) {
            createAcl(client, aclBinding);
        }
    }

    public static void createAcl(AdminClient client, AclBinding aclBinding) {
        createAcl(client, Collections.singletonList(aclBinding));
    }

    public static void createAcl(
            Map<String, Object> clientConfig,
            Collection<AclBinding> aclBindings) {
        try (AdminClient client = initClient(clientConfig)) {
            createAcl(client, aclBindings);
        }
    }

    public static void createAcl(AdminClient client, Collection<AclBinding> aclBindings) {
        Validate.notNull(client, "Admin client is null");
        Validate.notNull(aclBindings, "Collection of ACL bindings is null");
        Validate.noNullElements(aclBindings, "Collection of ACL bindings contains null elements");

        completeAndGet(client.createAcls(aclBindings).all());
    }

    public static Collection<AclBinding> describeAcl(
            Map<String, Object> clientConfig,
            AclBindingFilter aclBindingFilter) {
        try (AdminClient client = initClient(clientConfig)) {
           return describeAcl(client, aclBindingFilter);
        }
    }

    public static Collection<AclBinding> describeAcl(
            AdminClient client,
            AclBindingFilter aclBindingFilter) {
        Validate.notNull(client, "Admin client is null");
        Validate.notNull(aclBindingFilter, "ACL binding filter is null");

        return completeAndGet(client.describeAcls(aclBindingFilter).values());
    }

    public static Collection<AclBinding> deleteAcl(
            Map<String, Object> clientConfig,
            AclBindingFilter aclBindingFilter) {
        try (AdminClient client = initClient(clientConfig)) {
            return deleteAcl(client, aclBindingFilter);
        }
    }

    public static Collection<AclBinding> deleteAcl(
            AdminClient client,
            AclBindingFilter aclBindingFilter) {
        return deleteAcl(client, Collections.singletonList(aclBindingFilter));
    }

    public static Collection<AclBinding> deleteAcl(
            Map<String, Object> clientConfig,
            Collection<AclBindingFilter> aclBindingFilters) {
        try (AdminClient client = initClient(clientConfig)) {
            return deleteAcl(client, aclBindingFilters);
        }
    }

    public static Collection<AclBinding> deleteAcl(
            AdminClient client,
            Collection<AclBindingFilter> aclBindingFilters) {
        Validate.notNull(client, "Admin client is null");
        Validate.notNull(aclBindingFilters, "Collection fo ACL binding filters is null");
        Validate.noNullElements(aclBindingFilters, "Collection of ACL binding filters contains null elements");

        return completeAndGet(client.deleteAcls(aclBindingFilters).all());
    }

    public static void deleteRecords(
            Map<String, Object> clientConfig,
            Map<TopicPartition, Long> beforeOffsets) {
        try (AdminClient client = initClient(clientConfig)) {
            deleteRecords(client, beforeOffsets);
        }
    }

    public static void deleteRecords(AdminClient client, Map<TopicPartition, Long> beforeOffsets) {
        Validate.notNull(client, "Admin client is null");
        Validate.notEmpty(beforeOffsets, "Before-offsets map is null or empty");
        Validate.noNullElements(beforeOffsets.keySet(), "Before-offsets map contains null keys");
        Validate.noNullElements(beforeOffsets.values(), "Before-offsets map contains null values");

        Map<TopicPartition, RecordsToDelete> recordsToDelete = beforeOffsets.entrySet().stream().
                collect(
                        Collectors.toMap(
                                e -> e.getKey(),
                                e -> RecordsToDelete.beforeOffset(e.getValue())));

        completeAndGet(client.deleteRecords(recordsToDelete).all());
    }

    public static void deleteAllRecords(
            Map<String, Object> clientConfig,
            Collection<TopicPartition> topicPartitions) {
        try (AdminClient client = initClient(clientConfig)) {
            deleteAllRecords(client, topicPartitions);
        }
    }

    public static void deleteAllRecords(AdminClient client, Collection<TopicPartition> topicPartitions) {
        Validate.notNull(client, "Admin client is null");
        Validate.notEmpty(topicPartitions, "Collection of topic partitions is null or empty");
        Validate.noNullElements(topicPartitions, "Collection of topic partitions contains null elements");

        Map<TopicPartition, RecordsToDelete> recordsToDelete = topicPartitions.stream().
                collect(
                        Collectors.toMap(
                                Function.identity(),
                                tp -> RecordsToDelete.beforeOffset(-1)));

        completeAndGet(client.deleteRecords(recordsToDelete).all());
    }

    public static void deleteAllRecords(Map<String, Object> clientConfig, String topicName) {
        try (AdminClient client = initClient(clientConfig)) {
            deleteAllRecords(client, topicName);
        }
    }

    public static void deleteAllRecords(AdminClient client, String topicName) {
        Validate.notNull(client, "Admin client is null");
        Validate.notBlank(topicName, "Topic name is blank");

        TopicDescription topicDescription = describeTopic(client, topicName);

        Map<TopicPartition, RecordsToDelete> recordsToDelete = topicDescription.partitions().stream().
                collect(
                        Collectors.toMap(
                                tpi -> new TopicPartition(topicName, tpi.partition()),
                                tpi -> RecordsToDelete.beforeOffset(-1)));

        completeAndGet(client.deleteRecords(recordsToDelete).all());
    }

    public static boolean consumerGroupExists(Map<String, Object> clientConfig, String groupName) {
        try (AdminClient client = initClient(clientConfig)) {
            return consumerGroupExists(client, groupName);
        }
    }

    public static boolean consumerGroupExists(AdminClient client, String groupName) {
        return describeConsumerGroup(client, groupName) != null;
    }

    public static ConsumerGroupDescription describeConsumerGroup(Map<String, Object> clientConfig, String groupName) {
        try (AdminClient client = initClient(clientConfig)) {
            return describeConsumerGroup(client, groupName);
        }
    }

    public static ConsumerGroupDescription describeConsumerGroup(AdminClient client, String groupName) {
        Validate.notNull(client, "Admin client is null");
        Validate.notNull(groupName, "Group name is null"); // should be notBlank(...) but Kafka for some reason allows blank group ids...

        return describeConsumerGroups(client, Collections.singleton(groupName)).get(groupName);
    }

    public static Map<String, ConsumerGroupDescription> describeAllConsumerGroups(
            Map<String, Object> clientConfig) {
        try (AdminClient client = initClient(clientConfig)) {
            return describeAllConsumerGroups(client);
        }
    }

    public static Map<String, ConsumerGroupDescription> describeAllConsumerGroups(
            AdminClient client) {
        Collection<String> groupNames = listAllConsumerGroupNames(client);
        if (groupNames.isEmpty()) {
            return Collections.emptyMap();
        }

        return describeConsumerGroups(client, groupNames);
    }

    public static Map<String, ConsumerGroupDescription> describeConsumerGroups(
            Map<String, Object> clientConfig,
            Collection<String> groupNames) {
        try (AdminClient client = initClient(clientConfig)) {
            return describeConsumerGroups(client, groupNames);
        }
    }

    public static Map<String, ConsumerGroupDescription> describeAllConsumerGroupsInParallel(
            Map<String, Object> clientConfig) {
        Collection<String> groupNames = listAllConsumerGroupNames(clientConfig);
        if (groupNames.isEmpty()) {
            return Collections.emptyMap();
        }

        return describeConsumerGroupsInParallel(clientConfig, groupNames);
    }

    public static Map<String, ConsumerGroupDescription> describeConsumerGroupsInParallel(
            Map<String, Object> clientConfig,
            Collection<String> groupNames) {
        Validate.notNull(clientConfig, "Config is null");
        Validate.notEmpty(groupNames, "Collection of group names is null or empty");
        Validate.noNullElements(groupNames, "Collection of group names contains null elements");

        if (groupNames.size() <= PARALLELISM_THRESHOLD) {
            return describeConsumerGroups(clientConfig, groupNames);
        }

        return describeInParallel(
                clientConfig,
                "consumer group",
                groupNames,
                (client, names) -> client.describeConsumerGroups(names).describedGroups());
    }

    public static Map<String, ConsumerGroupDescription> describeConsumerGroups(
            AdminClient client,
            Collection<String> groupNames) {
        Validate.notNull(client, "Admin client is null");
        Validate.notEmpty(groupNames, "Collection of group names is null or empty");
        Validate.noNullElements(groupNames, "Collection of group names contains null elements");

        return completeAndGet(client.describeConsumerGroups(groupNames).all());
    }

    public static Collection<String> listAllConsumerGroupNames(Map<String, Object> clientConfig) {
        try (AdminClient client = initClient(clientConfig)) {
            return listAllConsumerGroupNames(client);
        }
    }

    public static Collection<String> listAllConsumerGroupNames(AdminClient client) {
        Collection<ConsumerGroupListing> groups = listConsumerGroups(client);
        return groups.stream().
                map(ConsumerGroupListing::groupId).
                collect(Collectors.toSet());
    }

    public static Collection<ConsumerGroupListing> listConsumerGroups(Map<String, Object> clientConfig) {
        try (AdminClient client = initClient(clientConfig)) {
            return listConsumerGroups(client);
        }
    }

    public static Collection<ConsumerGroupListing> listConsumerGroups(AdminClient client) {
        Validate.notNull(client, "Admin client is null");

        return completeAndGet(
                client.listConsumerGroups().all());
    }

    public static Map<String, Map<TopicPartition, OffsetAndMetadata>> listAllConsumerGroupOffsets(
            Map<String, Object> clientConfig) {
        try (AdminClient client = initClient(clientConfig)) {
            return listAllConsumerGroupOffsets(client);
        }
    }

    public static Map<String, Map<TopicPartition, OffsetAndMetadata>> listAllConsumerGroupOffsets(
            AdminClient client) {
        Collection<String> groupNames = listAllConsumerGroupNames(client);
        if (CollectionUtils.isEmpty(groupNames)) {
            return Collections.emptyMap();
        }

        Map<String, Map<TopicPartition, OffsetAndMetadata>> allOffsets =
                new HashMap<>((int)Math.ceil(groupNames.size() / 0.75f));
        for (String groupName : groupNames) {
            Map<TopicPartition, OffsetAndMetadata> offsets = listConsumerGroupOffsets(client, groupName);
            allOffsets.put(groupName, offsets);
        }
        return allOffsets;
    }

    public static Map<TopicPartition, OffsetAndMetadata> listConsumerGroupOffsets(
            Map<String, Object> clientConfig,
            String groupName,
            TopicPartition ... topicPartitions) {
        try (AdminClient client = initClient(clientConfig)) {
            return listConsumerGroupOffsets(client, groupName, topicPartitions);
        }
    }

    public static Map<TopicPartition, OffsetAndMetadata> listConsumerGroupOffsets(
            AdminClient client,
            String groupName,
            TopicPartition ... topicPartitions) {
        return listConsumerGroupOffsets(
                client,
                groupName,
                topicPartitions != null ? Arrays.asList(topicPartitions) : null);
    }

    public static Map<TopicPartition, OffsetAndMetadata> listConsumerGroupOffsets(
            Map<String, Object> clientConfig,
            String groupName,
            List<TopicPartition> topicPartitions) {
        try (AdminClient client = initClient(clientConfig)) {
            return listConsumerGroupOffsets(client, groupName, topicPartitions);
        }
    }

    public static Map<TopicPartition, OffsetAndMetadata> listConsumerGroupOffsets(
            AdminClient client,
            String groupName,
            List<TopicPartition> topicPartitions) {
        Validate.notNull(client, "Admin client is null");
        Validate.notNull(groupName, "Group name is null"); // should be notBlank(...) but Kafka for some reason allows blank group ids...
        if (!CollectionUtils.isEmpty(topicPartitions)) {
            Validate.noNullElements(topicPartitions, "Collection of topic partitions contains null elements");
        }

        ListConsumerGroupOffsetsOptions options = new ListConsumerGroupOffsetsOptions();
        options.topicPartitions(
                !CollectionUtils.isEmpty(topicPartitions) ? topicPartitions : null);

        return completeAndGet(
                client.listConsumerGroupOffsets(groupName, options).partitionsToOffsetAndMetadata());
    }

    public static Map<String, Map<TopicPartition, OffsetAndMetadata>> listAllConsumerGroupOffsetsInParallel(
            Map<String, Object> clientConfig) {
        Collection<String> groupNames = listAllConsumerGroupNames(clientConfig);
        if (groupNames.isEmpty()) {
            return Collections.emptyMap();
        }

        return listConsumerGroupOffsetsInParallel(clientConfig, groupNames);
    }

    public static Map<String, Map<TopicPartition, OffsetAndMetadata>> listConsumerGroupOffsetsInParallel(
            Map<String, Object> clientConfig,
            Collection<String> groupNames) {
        Validate.notNull(clientConfig, "Config is null");
        Validate.notEmpty(groupNames, "Collection of group names is null or empty");
        Validate.noNullElements(groupNames, "Collection of group names contains null elements");

        if (groupNames.size() <= PARALLELISM_THRESHOLD) {
            Map<String, Map<TopicPartition, OffsetAndMetadata>> allOffsets =
                    new HashMap<>((int)Math.ceil(groupNames.size() / 0.75f));
            for (String groupName : groupNames) {
                Map<TopicPartition, OffsetAndMetadata> offsets = listConsumerGroupOffsets(clientConfig, groupName);
                allOffsets.put(groupName, offsets);
            }
            return allOffsets;
        }

        return describeInParallel(
                clientConfig,
                "consumer group offset",
                groupNames,
                (client, names) -> {
                    Map<String, KafkaFuture<Map<TopicPartition, OffsetAndMetadata>>> futures =
                            new HashMap<>((int) (names.size() / 0.75));
                    for (String groupName : names) {
                        futures.put(
                                groupName,
                                client.listConsumerGroupOffsets(groupName).partitionsToOffsetAndMetadata());
                    }
                    return futures;
                });
    }

    public static void deleteConsumerGroup(Map<String, Object> clientConfig, String groupName) {
        try (AdminClient client = initClient(clientConfig)) {
            deleteConsumerGroup(client, groupName);
        }
    }

    public static void deleteConsumerGroup(AdminClient client, String groupName) {
        deleteConsumerGroups(client, Collections.singleton(groupName));
    }

    public static void deleteConsumerGroups(Map<String, Object> clientConfig, Collection<String> groupNames) {
        try (AdminClient client = initClient(clientConfig)) {
            deleteConsumerGroups(client, groupNames);
        }
    }

    public static void deleteConsumerGroups(AdminClient client, Collection<String> groupNames) {
        Validate.notNull(client, "Admin client is null");
        Validate.notEmpty(groupNames, "Collection of group names is null or empty");
        Validate.noNullElements(groupNames, "Collection of group names contains null elements");

        completeAndGet(client.deleteConsumerGroups(groupNames).all());
    }

    public static void electPreferredLeaders(Map<String, Object> clientConfig) {
        electPreferredLeaders(clientConfig, null);
    }

    public static void electPreferredLeaders(AdminClient client) {
        electPreferredLeaders(client, null);
    }

    public static void electPreferredLeaders(
            Map<String, Object> clientConfig,
            Collection<TopicPartition> partitions) {
        try (AdminClient client = initClient(clientConfig)) {
            electPreferredLeaders(client, partitions);
        }
    }

    public static void electPreferredLeaders(AdminClient client, Collection<TopicPartition> partitions) {
        Validate.notNull(client, "Admin client is null");

        Set<TopicPartition> partitionSet = null;
        if (partitions != null) {
            Validate.noNullElements(partitions, "Collection of topic partitions contains null elements");

            partitionSet = partitions instanceof Set ? (Set<TopicPartition>)partitions : new HashSet<>(partitions);
        }

        completeAndGet(
                client.electLeaders(ElectionType.PREFERRED, partitionSet).all());
    }

    public static void alterResourceConfigs(
            Map<String, Object> clientConfig,
            ConfigResource resource,
            Map<String, String> configs) {
        try (AdminClient client = initClient(clientConfig)) {
            alterResourceConfigs(client, resource, configs);
        }
    }

    public static void alterResourceConfigs(
            AdminClient client,
            ConfigResource resource,
            Map<String, String> configs) {
        Validate.notNull(resource, "Config resource is null");
        Validate.notNull(configs, "Map of configs is null");
        Validate.noNullElements(configs.keySet(), "Map of configs contains null keys");

        Collection<AlterConfigOp> configOps = new LinkedList<>();
        configs.forEach((name, value) -> {
            if (StringUtils.isBlank(value)) {
                configOps.add(asDeleteConfigOp(name));
            } else {
                configOps.add(asSetConfigOp(name, value));
            }
        });

        incrementalAlterResourceConfigs(client, resource, configOps);
    }

    public static void resetAllResourceConfigs(Map<String, Object> clientConfig, ConfigResource resource) {
        try (AdminClient client = initClient(clientConfig)) {
            resetAllResourceConfigs(client, resource);
        }
    }

    public static void resetAllResourceConfigs(AdminClient client, ConfigResource resource) {
        Validate.notNull(resource, "Config resource is null");

        AbstractConfigDef configDef = getConfigDef(resource);
        Collection<String> configNames = configDef.keys().stream().
                map(key -> key.name).
                collect(Collectors.toList());

        resetResourceConfigs(client, resource, configNames);
    }

    public static void resetResourceConfigs(
            Map<String, Object> clientConfig,
            ConfigResource resource,
            Collection<String> configNames) {
        try (AdminClient client = initClient(clientConfig)) {
            resetResourceConfigs(client, resource, configNames);
        }
    }

    public static void resetResourceConfigs(
            AdminClient client,
            ConfigResource resource,
            Collection<String> configNames) {
        Validate.notNull(resource, "Config resource is null");
        Validate.notNull(configNames, "Collection of config names is null");
        Validate.noNullElements(configNames, "Collection of config names contains null elements");

        Collection<AlterConfigOp> deleteOps = configNames.stream().
                map(AdminClientUtils::asDeleteConfigOp).
                collect(Collectors.toList());

        incrementalAlterResourceConfigs(client, resource, deleteOps);
    }

    public static void incrementalAlterResourceConfigs(
            Map<String, Object> clientConfig,
            ConfigResource resource,
            Collection<AlterConfigOp> configOps) {
        try (AdminClient client = initClient(clientConfig)) {
            incrementalAlterResourceConfigs(client, resource, configOps);
        }
    }

    public static void incrementalAlterResourceConfigs(
            AdminClient client,
            ConfigResource resource,
            Collection<AlterConfigOp> configOps) {
        Validate.notNull(resource, "Config resource is null");
        Validate.notNull(configOps, "Collection of config operations is null");
        Validate.noNullElements(configOps, "Collection of config operations contains null elements");

        incrementalAlterConfigs(client, Collections.singletonMap(resource, configOps));
    }

    public static void incrementalAlterConfigs(
            Map<String, Object> clientConfig,
            Map<ConfigResource, Collection<AlterConfigOp>> configs) {
        try (AdminClient client = initClient(clientConfig)) {
            incrementalAlterConfigs(client, configs);
        }
    }

    public static void incrementalAlterConfigs(
            AdminClient client,
            Map<ConfigResource, Collection<AlterConfigOp>> configs) {
        Validate.notNull(client, "Admin client is null");
        Validate.notNull(configs, "Map of configs is null");
        Validate.noNullElements(configs.keySet(), "Map of configs contains null keys");
        Validate.noNullElements(configs.values(), "Map of configs contains null values");

        completeAndGet(client.incrementalAlterConfigs(configs).all());
    }

    public static Map<String, String> configToMap(Config config) {
        return configToMap(config, true, true);
    }

    public static Map<String, String> configToMap(
            Config config,
            boolean ignoreDefaultConfigs,
            boolean ignoreReadOnlyConfigs) {
        Validate.notNull(config, "Config is null");

        Collection<ConfigEntry> entries = config.entries();
        Map<String, String> configMap = new HashMap<>((int) (entries.size() / 0.75));
        for (ConfigEntry entry : entries) {
            if (entry.isDefault() && ignoreDefaultConfigs) {
                continue;
            }
            if (entry.isReadOnly() && ignoreReadOnlyConfigs) {
                continue;
            }
            configMap.put(entry.name(), entry.value());
        }
        return configMap;
    }

    public static Config mapToConfig(Map<String, String> configMap) {
        Validate.notNull(configMap, "Config map is null");

        List<ConfigEntry> entries = new ArrayList<>();
        configMap.forEach((key, value) -> entries.add(new ConfigEntry(key, value)));
        return new Config(entries);
    }

    public static <R> R call(
            Map<String, Object> clientConfig,
            AdminClientCallable<R> callable) throws Exception {
        try (AdminClient client = initClient(clientConfig)) {
            return callable.call(client);
        }
    }

    public static <R> R callUnchecked(
            Map<String, Object> clientConfig,
            UncheckedAdminClientCallable<R> callable) {
        try (AdminClient client = initClient(clientConfig)) {
            return callable.call(client);
        }
    }

    public static AdminClient initClient(Map<String, Object> config) {
        Validate.notNull(config, "Config is null");

        return AdminClient.create(config);
    }

    public static void closeQuietly(AdminClient client) {
        if (client == null) {
            return;
        }
        try {
            client.close();
        } catch (Exception ex) {
            // ignore
        }
    }

    private static Map<ConfigResource, Config> describeConfigs(
            AdminClient client,
            Collection<ConfigResource> resources) {
        return completeAndGet(
                client.describeConfigs(resources).all());
    }

    private static <T> T completeAndGet(KafkaFuture<T> future) {
        try {
            return future.get();
        } catch (InterruptedException ie) {
            throw new RuntimeException(ie);
        } catch (ExecutionException ee) {
            Throwable cause = ee.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException)cause;
            } else {
                throw new RuntimeException(cause);
            }
        }
    }

    private static AlterConfigOp asSetConfigOp(String name, String value) {
        return new AlterConfigOp(new ConfigEntry(name, value), AlterConfigOp.OpType.SET);
    }

    private static AlterConfigOp asDeleteConfigOp(String name) {
        return new AlterConfigOp(new ConfigEntry(name, null), AlterConfigOp.OpType.DELETE);
    }

    private static AbstractConfigDef getConfigDef(ConfigResource resource) {
        return getConfigDef(resource.type());
    }

    private static AbstractConfigDef getConfigDef(ConfigResource.Type resourceType) {
        if (resourceType == Type.BROKER) {
            return BrokerConfigDef.INSTANCE;
        } else if (resourceType == Type.TOPIC) {
            return TopicConfigDef.INSTANCE;
        } else {
            throw new IllegalArgumentException("Unsupported resource type: " + resourceType);
        }
    }

    @SuppressWarnings("deprecation")
    private static Config createDefaultConfig(ConfigResource.Type resourceType) {
        AbstractConfigDef configDef = getConfigDef(resourceType);

        List<ConfigKey> keys = configDef.keys();

        List<ConfigEntry> entries = new ArrayList<>(keys.size());
        for (ConfigKey key : keys) {
            entries.add(new ConfigEntry(
                    key.name,
                    configDef.defaultValueAsString(key.name),
                    true,
                    false,
                    false));
        }
        return new Config(Collections.unmodifiableList(entries));
    }

    private static <D> Map<String, D> describeInParallel(
            Map<String, Object> clientConfig,
            String label,
            Collection<String> resourceNames,
            DescribeFunction<D> describeFunction) {
        label = StringUtils.isNotBlank(label) ? label : "resource";

        int numClients = Runtime.getRuntime().availableProcessors();

        LOGGER.info("Initiating parrallel description of {} {}s using {} clients", resourceNames.size(), label, numClients);

        List<AdminClient> clients = new ArrayList<>(numClients);
        try {
            for (int i = 0; i < numClients; i++) {
                clients.add(initClient(clientConfig));
            }

            List<List<String>> resourceNamePartitions = ListUtils.partition(
                    new ArrayList<>(resourceNames),
                    PARALLELISM_THRESHOLD);

            Map<String, KafkaFuture<D>> futures = new HashMap<>();

            for (int i = 0; i < resourceNamePartitions.size(); i++) {
                List<String> resourceNamePartition = resourceNamePartitions.get(i);

                AdminClient client = clients.get(i % numClients);
                futures.putAll(describeFunction.apply(client, resourceNamePartition));
            }

            int describedCount = 0;

            Map<String, D> descriptions = new HashMap<>((int) (resourceNames.size() / 0.75));
            for (Entry<String, KafkaFuture<D>> entry : futures.entrySet()) {
                descriptions.put(entry.getKey(), entry.getValue().get());

                describedCount++;

                if (describedCount % PARALLELISM_THRESHOLD == 0) {
                    LOGGER.info("Described {} {}s out of {}", describedCount, label, resourceNames.size());
                }
            }

            if (describedCount % PARALLELISM_THRESHOLD != 0) {
                LOGGER.info("Described {} {}s out of {}", describedCount, label, resourceNames.size());
            }

            return descriptions;
        } catch (InterruptedException | ExecutionException ex) {
            throw new RuntimeException("Failed to describe " + label + "s in parallel", ex);
        } finally {
            for (AdminClient client : clients) {
                client.close();
            }
        }
    }

    private static interface DescribeFunction<D> {
        Map<String, KafkaFuture<D>> apply(AdminClient client, Collection<String> resourceNames);
    }

}
