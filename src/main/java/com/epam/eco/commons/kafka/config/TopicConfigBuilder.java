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

import java.util.Map;

import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.TimestampType;

import com.epam.eco.commons.kafka.CleanupPolicy;

/**
 * @author Andrei_Tytsik
 */
public class TopicConfigBuilder extends AbstractConfigBuilder<TopicConfigBuilder> {

    private TopicConfigBuilder(Map<String, Object> properties) {
        super(properties);
    }

    public static TopicConfigBuilder with(Map<String, Object> properties) {
        return new TopicConfigBuilder(properties);
    }

    public static TopicConfigBuilder withEmpty() {
        return new TopicConfigBuilder(null);
    }

    public TopicConfigBuilder segmentBytes(int segmentBytes) {
        return property(TopicConfig.SEGMENT_BYTES_CONFIG, segmentBytes);
    }

    public TopicConfigBuilder segmentMs(long segmentMs) {
        return property(TopicConfig.SEGMENT_MS_CONFIG, segmentMs);
    }

    public TopicConfigBuilder segmentJitterMs(long segmentJitterMs) {
        return property(TopicConfig.SEGMENT_JITTER_MS_CONFIG, segmentJitterMs);
    }

    public TopicConfigBuilder segmentIndexBytes(int segmentIndexBytes) {
        return property(TopicConfig.SEGMENT_INDEX_BYTES_CONFIG, segmentIndexBytes);
    }

    public TopicConfigBuilder flushMessagesInterval(long flushMessagesInterval) {
        return property(TopicConfig.FLUSH_MESSAGES_INTERVAL_CONFIG, flushMessagesInterval);
    }

    public TopicConfigBuilder flushMs(long flushMs) {
        return property(TopicConfig.FLUSH_MS_CONFIG, flushMs);
    }

    public TopicConfigBuilder retentionBytes(long retentionBytes) {
        return property(TopicConfig.RETENTION_BYTES_CONFIG, retentionBytes);
    }

    public TopicConfigBuilder retentionMs(long retentionMs) {
        return property(TopicConfig.RETENTION_MS_CONFIG, retentionMs);
    }

    public TopicConfigBuilder maxMessageBytes(int maxMessageBytes) {
        return property(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, maxMessageBytes);
    }

    public TopicConfigBuilder indexIntervalBytes(int indexIntervalBytes) {
        return property(TopicConfig.INDEX_INTERVAL_BYTES_CONFIG, indexIntervalBytes);
    }

    public TopicConfigBuilder fileDeleteDelayMs(long fileDeleteDelayMs) {
        return property(TopicConfig.FILE_DELETE_DELAY_MS_CONFIG, fileDeleteDelayMs);
    }

    public TopicConfigBuilder deleteRetentionMs(long deleteRetentionMs) {
        return property(TopicConfig.DELETE_RETENTION_MS_CONFIG, deleteRetentionMs);
    }

    public TopicConfigBuilder minCompactionLagMs(long minCompactionLagMs) {
        return property(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, minCompactionLagMs);
    }

    public TopicConfigBuilder maxCompactionLagMs(long maxCompactionLagMs) {
        return property(TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG, maxCompactionLagMs);
    }

    public TopicConfigBuilder minCleanableDirtyRatio(double minCleanableDirtyRatio) {
        return property(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, minCleanableDirtyRatio);
    }

    public TopicConfigBuilder cleanupPolicyCompact() {
        return cleanupPolicy(CleanupPolicy.COMPACT);
    }

    public TopicConfigBuilder cleanupPolicyDelete() {
        return cleanupPolicy(CleanupPolicy.DELETE);
    }

    public TopicConfigBuilder cleanupPolicy(CleanupPolicy policy) {
        return cleanupPolicy(policy.name);
    }

    public TopicConfigBuilder cleanupPolicy(String policy) {
        return property(TopicConfig.CLEANUP_POLICY_CONFIG, policy);
    }

    public TopicConfigBuilder unclearLeaderElectionEnabled() {
        return unclearLeaderElectionEnable(true);
    }

    public TopicConfigBuilder unclearLeaderElectionDisabled() {
        return unclearLeaderElectionEnable(false);
    }

    public TopicConfigBuilder unclearLeaderElectionEnable(boolean unclearLeaderElectionEnable) {
        return property(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, unclearLeaderElectionEnable);
    }

    public TopicConfigBuilder minInSyncReplicas(int minInSyncReplicas) {
        return property(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, minInSyncReplicas);
    }

    public TopicConfigBuilder compressionType(CompressionType compressionType) {
        return compressionType(compressionType.name);
    }

    public TopicConfigBuilder compressionType(String compressionType) {
        return property(TopicConfig.COMPRESSION_TYPE_CONFIG, compressionType);
    }

    public TopicConfigBuilder preallocateEnabled() {
        return preallocate(true);
    }

    public TopicConfigBuilder preallocateDisabled() {
        return preallocate(false);
    }

    public TopicConfigBuilder preallocate(boolean preallocate) {
        return property(TopicConfig.PREALLOCATE_CONFIG, preallocate);
    }

    public TopicConfigBuilder messageTimestampType(TimestampType messageTimestampType) {
        return messageTimestampType(messageTimestampType.name);
    }

    public TopicConfigBuilder messageTimestampType(String messageTimestampType) {
        return property(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, messageTimestampType);
    }

    public TopicConfigBuilder messageDownconversionEnabled() {
        return messageDownconversionEnable(true);
    }

    public TopicConfigBuilder messageDownconversionDisabled() {
        return messageDownconversionEnable(false);
    }

    public TopicConfigBuilder messageDownconversionEnable(boolean messageDownconversionEnable) {
        return property(TopicConfig.MESSAGE_DOWNCONVERSION_ENABLE_CONFIG, messageDownconversionEnable);
    }

}
