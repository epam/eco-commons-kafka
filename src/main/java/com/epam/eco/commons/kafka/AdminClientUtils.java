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
package com.epam.eco.commons.kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.admin.AdminClient;
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
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.ConfigKey;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import com.epam.eco.commons.kafka.config.TopicConfigDef;


/**
 * @author Andrei_Tytsik
 */
public abstract class AdminClientUtils {

    public static final Config TOPIC_DEFAULT_CONFIG = createTopicDefaultConfig();

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

    public static void resetTopicConfig(
            Map<String, Object> clientConfig,
            String topicName) {
        try (AdminClient client = initClient(clientConfig)) {
            alterTopicConfig(client, topicName, Collections.emptyMap());
        }
    }

    public static void alterTopicConfig(
            Map<String, Object> clientConfig,
            String topicName,
            Map<String, String> configMap) {
        try (AdminClient client = initClient(clientConfig)) {
            alterTopicConfig(client, topicName, configMap);
        }
    }

    public static void alterTopicConfig(
            AdminClient client,
            String topicName,
            Map<String, String> configMap) {
        Validate.notNull(client, "Admin client is null");
        Validate.notBlank(topicName, "Topic name is blank");

        ConfigResource resource = new ConfigResource(Type.TOPIC, topicName);
        Config configs = mapToConfig(configMap);

        completeAndGet(
                client.alterConfigs(Collections.singletonMap(resource, configs)).all());
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
        Validate.notBlank(groupName, "Group name is blank");

        return describeConsumerGroups(client, Collections.singleton(groupName)).get(groupName);
    }

    public static Map<String, ConsumerGroupDescription> describeConsumerGroups(
            Map<String, Object> clientConfig,
            Collection<String> groupNames) {
        try (AdminClient client = initClient(clientConfig)) {
            return describeConsumerGroups(client, groupNames);
        }
    }

    public static Map<String, ConsumerGroupDescription> describeConsumerGroups(
            AdminClient client,
            Collection<String> groupNames) {
        Validate.notNull(client, "Admin client is null");
        Validate.notEmpty(groupNames, "Collection of group names is null or empty");
        Validate.noNullElements(groupNames, "Collection of group names contains null elements");

        return completeAndGet(client.describeConsumerGroups(groupNames).all());
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
        Validate.notBlank(groupName, "Group name is blank");
        if (!CollectionUtils.isEmpty(topicPartitions)) {
            Validate.noNullElements(topicPartitions, "Collection of topic partitions contains null elements");
        }

        ListConsumerGroupOffsetsOptions options = new ListConsumerGroupOffsetsOptions();
        options.topicPartitions(
                !CollectionUtils.isEmpty(topicPartitions) ? topicPartitions : null);

        return completeAndGet(
                client.listConsumerGroupOffsets(groupName, options).partitionsToOffsetAndMetadata());
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

    public interface AdminClientCallable<R> {
        R call(AdminClient client) throws Exception;
    }

    public interface UncheckedAdminClientCallable<R> {
        R call(AdminClient client);
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

    @SuppressWarnings("deprecation")
    private static Config createTopicDefaultConfig() {
        List<ConfigEntry> entries = new ArrayList<>(TopicConfigDef.INSTANCE.keys().size());
        for (ConfigKey key : TopicConfigDef.INSTANCE.keys()) {
            entries.add(new ConfigEntry(
                    key.name,
                    getConfigDefaultValue(key),
                    true,
                    false,
                    false));
        }
        return new Config(Collections.unmodifiableList(entries));
    }

    private static String getConfigDefaultValue(ConfigKey key) {
        if (!key.hasDefault()) {
            return "";
        }

        if (key.defaultValue == null) {
            return "null";
        }

        String defaultValueStr = ConfigDef.convertToString(key.defaultValue, key.type);
        if (defaultValueStr.isEmpty()) {
            return "\"\"";
        } else {
            return defaultValueStr;
        }
    }

}