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
package com.epam.eco.commons.kafka.producer.tx;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.errors.ProducerFencedException;

import java.util.Random;

/**
 * @author Aliaksei_Valyaev
 */
public class TxMockProducer<K, V> extends MockProducer<K, V> {

    private static final double ERROR_PROBABILITY = 2.0 / 3;
    private static final long DELAY_SIZE = 50;

    private volatile boolean doErrorOnSend;
    private volatile boolean doErrorOnCommit;
    private volatile boolean doDelays;
    private volatile boolean txAborted;

    @Override
    public synchronized boolean errorNext(RuntimeException e) {
        if (e == null && isError()) {
            e = new RuntimeException();
        }
        if (doDelays) {
            try {
                Thread.sleep(DELAY_SIZE);
            } catch (InterruptedException ie) {
                throw new RuntimeException(ie);
            }
        }
        return super.errorNext(e);
    }

    @Override
    public void beginTransaction() throws ProducerFencedException {
        super.beginTransaction();
    }

    @Override
    public void commitTransaction() throws ProducerFencedException {
        if (doErrorOnCommit) {
            throw new RuntimeException();
        }
        if (txAborted) {
            throw new RuntimeException();
        }
        super.commitTransaction();
    }

    @Override
    public void abortTransaction() throws ProducerFencedException {
        txAborted = true;
        super.abortTransaction();
    }

    @Override
    public boolean transactionAborted() {
        return super.transactionAborted() || txAborted;
    }

    void doErrorOnSend() {
        doErrorOnSend = true;
    }

    void doErrorOnCommit(boolean value) {
        doErrorOnCommit = value;
    }

    void clearTxAborted() {
        this.txAborted = false;
    }

    void doDelays() {
        doDelays = true;
    }

    @Override
    public synchronized void clear() {
        doErrorOnSend = false;
        doDelays = false;
        doErrorOnCommit = false;
        clearTxAborted();
        super.clear();
    }

    private boolean isError() {
        return doErrorOnSend && new Random().nextFloat() < ERROR_PROBABILITY;
    }
}
