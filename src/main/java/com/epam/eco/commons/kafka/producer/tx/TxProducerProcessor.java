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

import java.util.ArrayList;
import java.util.List;

/**
 * @author Aliaksei_Valyaev
 */
final class TxProducerProcessor<K, V> {

    private static final int DEFAULT_INIT_SIZE = 1000;

    private final TxProducerProps props;
    private final TxProducerMsgSender<K, V> sender;
    private final TxProducerTask task;

    private final List<KafkaTxMsg<K, V>> msgs;

    TxProducerProcessor(
            TxProducerProps props,
            TxProducerTask task,
            TxProducerMsgSender<K, V> sender
    ) {
        this.props = props;
        this.task = task;
        this.sender = sender;
        this.msgs = new ArrayList<>(initialListSize());
    }

    synchronized TxProducerProcessor<K, V> init() {
        task.setProcessor(this).schedule();
        return this;
    }

    synchronized void addMsg(KafkaTxMsg<K, V> msg) {
        msgs.add(msg);
        if (props.getMaxMsgsSize() > 0 && msgs.size() >= props.getMaxMsgsSize()) {
            process();
        }
    }

    synchronized void process() {
        if (sender.send(msgs)) {
            clearMsgs();
        }
    }

    synchronized void close() {
        task.close();
        sender.close();
    }

    private void clearMsgs() {
        msgs.clear();
    }

    private int initialListSize() {
        return props.getMaxMsgsSize() > 0 ? props.getMaxMsgsSize() : DEFAULT_INIT_SIZE;
    }

}
