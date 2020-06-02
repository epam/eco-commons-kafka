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
package com.epam.eco.commons.kafka.producer.tx;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Aliaksei_Valyaev
 */
public final class TxProducerProps {

    private final String consumerGroup;
    private final String txIdPrefix;
    private final int maxMsgsSize;
    private final long sendDelayMs;
    private final long sendPeriodMs;
    private final int threadPoolSize;
    private final Map<String, Object> producerCfg;

    TxProducerProps(
            String consumerGroup,
            String txIdPrefix,
            int maxMsgsSize,
            long sendDelayMs,
            long sendPeriodMs,
            int threadPoolSize,
            Map<String, Object> producerCfg
    ) {
        this.consumerGroup = consumerGroup;
        this.txIdPrefix = txIdPrefix;
        this.maxMsgsSize = maxMsgsSize;
        this.sendDelayMs = sendDelayMs;
        this.sendPeriodMs = sendPeriodMs;
        this.threadPoolSize = threadPoolSize;
        this.producerCfg = producerCfg;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public String getTxIdPrefix() {
        return txIdPrefix;
    }

    public int getMaxMsgsSize() {
        return maxMsgsSize;
    }

    public long getSendDelayMs() {
        return sendDelayMs;
    }

    public long getSendPeriodMs() {
        return sendPeriodMs;
    }

    public int getThreadPoolSize() {
        return threadPoolSize;
    }

    public Map<String, Object> getProducerCfg() {
        return producerCfg;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String consumerGroup;
        private String txIdPrefix;
        private int maxMsgsSize;
        private long sendDelayMs;
        private long sendPeriodMs;
        private int threadPoolSize;
        private Map<String, Object> producerCfg = new HashMap<>();

        private Builder() {
        }

        public Builder consumerGroup(String consumerGroup) {
            this.consumerGroup = consumerGroup;
            return this;
        }

        public Builder txIdPrefix(String txIdPrefix) {
            this.txIdPrefix = txIdPrefix;
            return this;
        }

        public Builder maxMsgsSize(int maxMsgsSize) {
            this.maxMsgsSize = maxMsgsSize;
            return this;
        }

        public Builder sendDelayMs(long sendDelayMs) {
            this.sendDelayMs = sendDelayMs;
            return this;
        }

        public Builder sendPeriodMs(long sendPeriodMs) {
            this.sendPeriodMs = sendPeriodMs;
            return this;
        }

        public Builder threadPoolSize(int threadPoolSize) {
            this.threadPoolSize = threadPoolSize;
            return this;
        }

        public Builder producerCfg(Map<String, Object> producerCfg) {
            this.producerCfg.putAll(producerCfg);
            return this;
        }

        public Builder producerCfg(String key, Object value) {
            this.producerCfg.put(key, value);
            return this;
        }

        public TxProducerProps build() {
            return new TxProducerProps(
                    consumerGroup,
                    txIdPrefix,
                    maxMsgsSize,
                    sendDelayMs,
                    sendPeriodMs,
                    threadPoolSize,
                    producerCfg
            );
        }
    }

}
