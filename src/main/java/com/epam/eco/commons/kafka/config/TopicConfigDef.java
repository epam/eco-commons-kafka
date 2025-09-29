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

import java.util.Collections;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Range;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.ValidList;
import org.apache.kafka.common.config.ConfigDef.ValidString;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.TopicConfig;

/**
 * @author Andrei_Tytsik
 */
public class TopicConfigDef extends AbstractConfigDef {

    public static final TopicConfigDef INSTANCE = new TopicConfigDef();
    private static final String TOPIC_CONFIGURATION_GROUP = "Topic Specific Configuration";

    private TopicConfigDef() {
        super(createDef());
    }

    private static ConfigDef createDef() {
        return new ConfigDef()
            .define(TopicConfig.CLEANUP_POLICY_CONFIG, Type.LIST, TopicConfig.CLEANUP_POLICY_DELETE, ValidList.in(TopicConfig.CLEANUP_POLICY_DELETE, TopicConfig.CLEANUP_POLICY_COMPACT), Importance.MEDIUM, TopicConfig.CLEANUP_POLICY_DOC, TOPIC_CONFIGURATION_GROUP, 1, Width.MEDIUM, "Cleanup Policy")
            .define(TopicConfig.COMPRESSION_TYPE_CONFIG, Type.STRING, "producer", ValidString.in("uncompressed", "zstd", "lz4", "snappy", "gzip", "producer"), Importance.MEDIUM, TopicConfig.COMPRESSION_TYPE_DOC, TOPIC_CONFIGURATION_GROUP, 2, Width.MEDIUM, "Compression Type")
            .define(TopicConfig.DELETE_RETENTION_MS_CONFIG, Type.LONG, 86400000L, Range.atLeast(0), Importance.MEDIUM, TopicConfig.DELETE_RETENTION_MS_DOC, TOPIC_CONFIGURATION_GROUP, 3, Width.MEDIUM, "Delete Retention Time")
            .define(TopicConfig.FILE_DELETE_DELAY_MS_CONFIG, Type.LONG, 60000L, Range.atLeast(0), Importance.MEDIUM, TopicConfig.FILE_DELETE_DELAY_MS_DOC, TOPIC_CONFIGURATION_GROUP, 4, Width.MEDIUM, "File Delete Delay Time")
            .define(TopicConfig.FLUSH_MESSAGES_INTERVAL_CONFIG, Type.LONG, Long.MAX_VALUE, Range.atLeast(0), Importance.MEDIUM, TopicConfig.FLUSH_MESSAGES_INTERVAL_DOC, TOPIC_CONFIGURATION_GROUP, 5, Width.MEDIUM, "Flush Messages")
            .define(TopicConfig.FLUSH_MS_CONFIG, Type.LONG, Long.MAX_VALUE, Range.atLeast(0), Importance.MEDIUM, TopicConfig.FLUSH_MS_DOC, TOPIC_CONFIGURATION_GROUP, 6, Width.MEDIUM, "Flush Time")
            .define("follower.replication.throttled.replicas", Type.LIST, Collections.emptyList(), Importance.MEDIUM, "A list of replicas for which log replication should be throttled on the follower side. The list should describe a set of replicas in the form [PartitionId]:[BrokerId],[PartitionId]:[BrokerId]:... or alternatively the wildcard '*' can be used to throttle all replicas for this topic.", TOPIC_CONFIGURATION_GROUP, 7, Width.MEDIUM, "Throttled Replicas")
            .define(TopicConfig.INDEX_INTERVAL_BYTES_CONFIG, Type.INT, 4096, Range.atLeast(0), Importance.MEDIUM, TopicConfig.INDEX_INTERVAL_BYTES_DOC, TOPIC_CONFIGURATION_GROUP, 8, Width.MEDIUM, "Index Interval")
            .define("leader.replication.throttled.replicas", Type.LIST, Collections.emptyList(), Importance.MEDIUM,"A list of replicas for which log replication should be throttled on the leader side. The list should describe a set of replicas in the form [PartitionId]:[BrokerId],[PartitionId]:[BrokerId]:... or alternatively the wildcard '*' can be used to throttle all replicas for this topic.", TOPIC_CONFIGURATION_GROUP, 9, Width.MEDIUM, "Throttled Replicas")
            .define(TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG, Type.LONG, Long.MAX_VALUE, Range.atLeast(0), Importance.MEDIUM, TopicConfig.MAX_COMPACTION_LAG_MS_DOC, TOPIC_CONFIGURATION_GROUP, 10, Width.MEDIUM, "Maximum Compaction Lag Time")
            .define(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, Type.INT, 1048588, Range.atLeast(0), Importance.MEDIUM, TopicConfig.MAX_MESSAGE_BYTES_DOC, TOPIC_CONFIGURATION_GROUP, 11, Width.MEDIUM, "Max Message Size")
            .define(TopicConfig.MESSAGE_DOWNCONVERSION_ENABLE_CONFIG, Type.BOOLEAN, Boolean.TRUE, Importance.MEDIUM, TopicConfig.MESSAGE_DOWNCONVERSION_ENABLE_DOC, TOPIC_CONFIGURATION_GROUP, 12, Width.MEDIUM, "Message Down-Conversion Enable")
            .define(TopicConfig.MESSAGE_TIMESTAMP_AFTER_MAX_MS_CONFIG, Type.LONG, Long.MAX_VALUE, Range.atLeast(0), Importance.MEDIUM, TopicConfig.MESSAGE_TIMESTAMP_AFTER_MAX_MS_DOC, TOPIC_CONFIGURATION_GROUP, 14, Width.MEDIUM, "Maximum Timestamp Difference")
            .define(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, Type.STRING, "CreateTime", ValidString.in("CreateTime", "LogAppendTime"), Importance.MEDIUM, TopicConfig.MESSAGE_TIMESTAMP_TYPE_DOC, TOPIC_CONFIGURATION_GROUP, 15, Width.MEDIUM, "Message Timestamp Type")
            .define(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, Type.DOUBLE, 0.5, Range.between(0, 1), Importance.MEDIUM, TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_DOC, TOPIC_CONFIGURATION_GROUP, 16, Width.MEDIUM, "Minimum Cleanable Dirty Ratio")
            .define(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, Type.LONG, 0L, Range.atLeast(0), Importance.MEDIUM, TopicConfig.MIN_COMPACTION_LAG_MS_DOC, TOPIC_CONFIGURATION_GROUP, 17, Width.MEDIUM, "Minimum Compaction Lag Time")
            .define(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, Type.INT, 1, Range.atLeast(1), Importance.MEDIUM, TopicConfig.MIN_IN_SYNC_REPLICAS_DOC, TOPIC_CONFIGURATION_GROUP, 18, Width.MEDIUM, "Minimum In-Sync Replicas")
            .define(TopicConfig.PREALLOCATE_CONFIG, Type.BOOLEAN, Boolean.FALSE, Importance.MEDIUM,
                    TopicConfig.PREALLOCATE_DOC, TOPIC_CONFIGURATION_GROUP, 19, Width.MEDIUM, "Pre-allocate Enable")
            .define(TopicConfig.RETENTION_BYTES_CONFIG, Type.LONG, -1L, Importance.MEDIUM, TopicConfig.RETENTION_BYTES_DOC, TOPIC_CONFIGURATION_GROUP, 20, Width.MEDIUM, "Retention Bytes")
            .define(TopicConfig.RETENTION_MS_CONFIG, Type.LONG, 604800000L, Range.atLeast(-1), Importance.MEDIUM, TopicConfig.RETENTION_MS_DOC, TOPIC_CONFIGURATION_GROUP, 21, Width.MEDIUM, "Retention Time")
            .define(TopicConfig.SEGMENT_BYTES_CONFIG, Type.INT, 1073741824, Range.atLeast(14), Importance.MEDIUM, TopicConfig.SEGMENT_BYTES_DOC, TOPIC_CONFIGURATION_GROUP, 22, Width.MEDIUM, "Segment Size")
            .define(TopicConfig.SEGMENT_INDEX_BYTES_CONFIG, Type.INT, 10485760, Range.atLeast(0), Importance.MEDIUM, TopicConfig.SEGMENT_INDEX_BYTES_DOC, TOPIC_CONFIGURATION_GROUP, 23, Width.MEDIUM, "Segment Index Size")
            .define(TopicConfig.SEGMENT_JITTER_MS_CONFIG, Type.LONG, 0L, Range.atLeast(0), Importance.MEDIUM, TopicConfig.SEGMENT_JITTER_MS_DOC, TOPIC_CONFIGURATION_GROUP, 24, Width.MEDIUM, "Segment Jitter Time")
            .define(TopicConfig.SEGMENT_MS_CONFIG, Type.LONG, 604800000L, Range.atLeast(1), Importance.MEDIUM, TopicConfig.SEGMENT_MS_DOC, TOPIC_CONFIGURATION_GROUP, 25, Width.MEDIUM, "Segment Time")
            .define(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, Type.BOOLEAN, Boolean.FALSE,
                    Importance.MEDIUM, TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_DOC, TOPIC_CONFIGURATION_GROUP, 26, Width.MEDIUM, "Unclean Leader Election Enable");
    }

}
