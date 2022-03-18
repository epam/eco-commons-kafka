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
package com.epam.eco.commons.kafka.producer.tx;

import org.apache.commons.lang3.Validate;
import org.apache.kafka.common.TopicPartition;

/**
 * @author Aliaksei_Valyaev
 */
public final class KafkaTxMsg<K, V> {

    private final String outTopic;
    private final TopicPartition inTopic;
    private final long offset;
    private final K key;
    private final V value;

    public KafkaTxMsg(String outTopic, TopicPartition inTopic, long offset, K key, V value) {
        Validate.notEmpty(outTopic, "empty outTopic");
        Validate.notNull(inTopic, "null inTopic");
        Validate.notEmpty(inTopic.topic(), "empty inTopic");
        Validate.isTrue(inTopic.partition() >= 0, "invalid inTopic partition");
        Validate.isTrue(offset >= 0, "invalid offset");
        Validate.notNull(key, "null key");

        this.outTopic = outTopic;
        this.inTopic = inTopic;
        this.offset = offset;
        this.key = key;
        this.value = value;
    }

    public String getOutTopic() {
        return outTopic;
    }

    public TopicPartition getInTopic() {
        return inTopic;
    }

    public long getOffset() {
        return offset;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }

    public static <K, V> Builder<K, V> builder() {
        return new Builder<>();
    }

    public static final class Builder<K, V> {
        private String outTopic;
        private TopicPartition inTopic;
        private long offset;
        private K key;
        private V value;

        private Builder() {
        }

        public Builder<K, V> outTopic(String outTopic) {
            this.outTopic = outTopic;
            return this;
        }

        public Builder<K, V> inTopic(TopicPartition inTopic) {
            this.inTopic = inTopic;
            return this;
        }

        public Builder<K, V> offset(long offset) {
            this.offset = offset;
            return this;
        }

        public Builder<K, V> key(K key) {
            this.key = key;
            return this;
        }

        public Builder<K, V> value(V value) {
            this.value = value;
            return this;
        }

        public KafkaTxMsg<K, V> build() {
            return new KafkaTxMsg<>(outTopic, inTopic, offset, key, value);
        }
    }

}
