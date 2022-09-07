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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.commons.lang3.Validate;

/**
 * @author Andrei_Tytsik
 */
public class BootstrapConsumerExecutor<K, V, R> {

    private static final String THREAD_NAME_PATTERN = "btsp-%s(%d)-%d";
    private static final AtomicInteger EXECUTOR_INDEX = new AtomicInteger(0);

    private final List<BootstrapConsumerThread<K, V, R>> threads;

    public BootstrapConsumerExecutor(
            BootstrapConsumer.Builder<K, V, R> consumerBuilder,
            Supplier<RecordCollector<K, V, R>> recordCollectorSupplier,
            int instanceCount,
            Consumer<R> handler) {
        Validate.notNull(consumerBuilder, "Bootstrap Consumer builder is null");
        Validate.notNull(recordCollectorSupplier, "RecordCollector supplier is null");
        Validate.isTrue(instanceCount > 0, "Instance count is invalid");
        Validate.notNull(handler, "Handler is null");

        threads = initThreads(
                consumerBuilder,
                recordCollectorSupplier,
                instanceCount,
                handler);
    }

    public void start() {
        for (BootstrapConsumerThread<K, V, R> thread : threads) {
            thread.start();
        }
    }

    public void waitForBootstrap() throws InterruptedException {
        for (BootstrapConsumerThread<K, V, R> thread : threads) {
            thread.waitForBootstrap();
        }
    }

    public void waitForBootstrap(long timeout, TimeUnit timeUnit) throws InterruptedException {
        for (BootstrapConsumerThread<K, V, R> thread : threads) {
            thread.waitForBootstrap(timeout, timeUnit);
        }
    }

    public void shutdown() throws InterruptedException {
        for (BootstrapConsumerThread<K, V, R> thread : threads) {
            thread.shutdown();
        }
    }

    private static <K, V, R> List<BootstrapConsumerThread<K, V, R>> initThreads(
            BootstrapConsumer.Builder<K, V, R> builder,
            Supplier<RecordCollector<K, V, R>> recordCollectorSupplier,
            int instanceCount,
            Consumer<R> handler) {
        int executorIndex = getExecutorIndex();
        List<BootstrapConsumerThread<K, V, R>> threads = new ArrayList<>(instanceCount);
        for (int instanceIndex = 0; instanceIndex < instanceCount; instanceIndex++) {
            BootstrapConsumer<K, V, R> consumer = builder.
                    recordCollector(recordCollectorSupplier.get()).
                    instanceCount(instanceCount).
                    instanceIndex(instanceIndex).
                    build();

            BootstrapConsumerThread<K, V, R> thread = new BootstrapConsumerThread<>(
                    consumer,
                    handler);

            thread.setName(
                    formatThreadName(consumer.getTopicName(), executorIndex, instanceIndex));
            threads.add(thread);
        }
        return threads;
    }

    private static String formatThreadName(String topicName, int executorIndex, int instanceIndex) {
        return String.format(THREAD_NAME_PATTERN, topicName, executorIndex, instanceIndex);
    }

    private static int getExecutorIndex() {
        return EXECUTOR_INDEX.getAndIncrement();
    }

    public static <K, V, R> BootstrapConsumerExecutor<K, V, R> with(
            BootstrapConsumer.Builder<K, V, R> consumerBuilder,
            Supplier<RecordCollector<K, V, R>> recordCollectorSupplier,
            int instanceCount,
            Consumer<R> handler) {
        return new BootstrapConsumerExecutor<>(
                consumerBuilder,
                recordCollectorSupplier,
                instanceCount,
                handler);
    }

}
