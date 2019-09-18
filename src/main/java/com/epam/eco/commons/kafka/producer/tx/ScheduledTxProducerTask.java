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

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author Aliaksei_Valyaev
 */
@SuppressWarnings("rawtypes")
final class ScheduledTxProducerTask implements TxProducerTask {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private static final float DELAY_DEVIATION_RATE = 1f;
    private static final float SEND_DEVIATION_RATE = 0.2f;

    private final ScheduledExecutorService executorService;
    private final TxProducerProps props;
    private volatile TxProducerProcessor processor;
    private volatile ScheduledFuture future;

    ScheduledTxProducerTask(ScheduledExecutorService executorService, TxProducerProps props) {
        Validate.notNull(executorService, "null executorService");
        Validate.notNull(props, "null props");

        this.executorService = executorService;
        this.props = props;
    }

    @Override
    public ScheduledTxProducerTask setProcessor(TxProducerProcessor processor) {
        Validate.notNull(processor, "null processor");

        this.processor = processor;
        return this;
    }

    @Override
    public void run() {
        processor.process();
    }

    @Override
    public void schedule() {
        long delay = props.getSendDelayMs()
                + calculateDeviation(props.getSendDelayMs(), DELAY_DEVIATION_RATE);
        long period = props.getSendPeriodMs()
                + calculateDeviation(props.getSendPeriodMs(), SEND_DEVIATION_RATE);

        log.info("scheduling processor task with delay: {} and period: {}", delay, period);

        // prohibit concurrent executions
        future = executorService.scheduleAtFixedRate(
                this,
                delay,
                period,
                TimeUnit.MILLISECONDS
        );
    }

    private long calculateDeviation(long origin, float rate) {
        return Math.round(new SecureRandom().nextInt((int) origin) * rate);
    }

    @Override
    public void close() {
        if (future != null && !future.isCancelled()) {
            future.cancel(true);
        }
    }
}
