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
package com.epam.eco.commons.kafka.consumer.bootstrap;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Andrei_Tytsik
 */
class BootstrapConsumerThread<K, V, R> extends Thread {

    private static final Logger log = LoggerFactory.getLogger(BootstrapConsumerThread.class);

    private static final long SHUTDOWN_TIMEOUT_SECONDS = 30;

    private final BootstrapConsumer<K, V, R> consumer;
    private final Consumer<R> handler;

    private final CountDownLatch bootstrapLatch = new CountDownLatch(1);

    private final AtomicBoolean running = new AtomicBoolean(true);
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    private final AtomicReference<RuntimeException> exception = new AtomicReference<>();

    public BootstrapConsumerThread(BootstrapConsumer<K, V, R> consumer, Consumer<R> handler) {
        Validate.notNull(consumer, "Bootstrap Consumer is null");
        Validate.notNull(handler, "Handler is null");

        this.consumer = consumer;
        this.handler = handler;
    }

    @Override
    public void run() {
        try {
            while (running.get()) {
                handler.accept(consumer.fetch());
                bootstrapLatch.countDown();
            }
        } catch (Exception e) {
            BootstrapException wrapped = toBootstrapException(e);
            log.error(wrapped.getMessage(), wrapped);
            exception.set(wrapped);
            // Trigger latch to rethrow exception immediately
            bootstrapLatch.countDown();
        } finally {
            running.set(false);
            consumer.close();
            shutdownLatch.countDown();
        }
    }

    public void shutdown() throws InterruptedException {
        shutdown(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    public void shutdown(long timeout, TimeUnit timeUnit) throws InterruptedException {
        if (running.compareAndSet(true, false)) {
            consumer.wakeup();
            shutdownLatch.await(timeout, timeUnit);
        }
    }

    public boolean waitForBootstrap(long timeout, TimeUnit timeUnit) throws InterruptedException {
        boolean result = bootstrapLatch.await(timeout, timeUnit);
        if (exception.get() != null) {
            throw exception.get();
        }
        return result;
    }

    public boolean waitForBootstrap() throws InterruptedException {
        return waitForBootstrap((long)(consumer.getBootstrapTimeoutInMs() * 1.5), TimeUnit.MILLISECONDS);
    }

    private BootstrapException toBootstrapException(Exception e) {
        if (e instanceof BootstrapException bootstrapException) {
            return bootstrapException;
        } else {
            return new BootstrapException("Error occurred while bootstrapping the data", e);
        }
    }

}
