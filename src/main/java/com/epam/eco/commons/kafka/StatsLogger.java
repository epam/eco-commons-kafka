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
package com.epam.eco.commons.kafka;

import java.io.Closeable;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Andrei_Tytsik
 */
public class StatsLogger implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(StatsLogger.class);

    private static final long LOG_INTERVAL_MIN = 1 * 1000;
    private static final long LOG_INTERVAL_MAX = 60 * 60 * 1000;
    private static final long LOG_INTERVAL_DEFAULT = 30 * 1000;
    private static final String THREAD_NAME_DEFAULT = "Stats";

    private final LogTask logTask;
    private final AtomicLong successfulCount = new AtomicLong(0);
    private final AtomicLong failedCount = new AtomicLong(0);

    public StatsLogger() {
        this(null, LOG_INTERVAL_DEFAULT);
    }

    public StatsLogger(String threadName) {
        this(threadName, LOG_INTERVAL_DEFAULT);
    }

    public StatsLogger(long logIntervalMs) {
        this(null, logIntervalMs);
    }

    public StatsLogger(String threadName, long logIntervalMs) {
        validateLogInterval(logIntervalMs);

        logTask = new LogTask(threadName, logIntervalMs);
    }

    public void report(long successfulCount, long failedCount) {
        reportSuccessful(successfulCount);
        reportFailed(failedCount);
    }

    public void incSuccessful() {
        reportSuccessful(1);
    }

    public void reportSuccessful(long count) {
        this.successfulCount.addAndGet(count);
    }

    public void incFailed() {
        reportFailed(1);
    }

    public void reportFailed(long count) {
        this.failedCount.addAndGet(count);
    }

    public Stats getStats() {
        return new Stats(successfulCount.get(), failedCount.get());
    }

    @Override
    public void close() {
        destroyLogTask();
    }

    private void destroyLogTask() {
        logTask.close();
    }

    private void validateLogInterval(long logInterval) {
        if (logInterval < LOG_INTERVAL_MIN || logInterval > LOG_INTERVAL_MAX) {
            throw new IllegalArgumentException(
                    String.format(
                            "Log interval %d is invalid, should be in range [%d..%d]",
                            logInterval, LOG_INTERVAL_MIN, LOG_INTERVAL_MAX));
        }
    }

    public static class Stats {

        private final long successfulCount;
        private final long failedCount;
        private final long totalCount;
        private final long successRate;

        Stats(long successfulCount, long failedCount) {
            this.successfulCount = successfulCount;
            this.failedCount = failedCount;

            totalCount = successfulCount + failedCount;
            successRate =
                    totalCount > 0 ?
                    (long)(((double)successfulCount / (double)totalCount) * 100) :
                    0;
        }

        public long getSuccessfulCount() {
            return successfulCount;
        }
        public long getFailedCount() {
            return failedCount;
        }
        public long getTotalCount() {
            return totalCount;
        }
        public long getSuccessRate() {
            return successRate;
        }

        @Override
        public String toString() {
            return String.format(
                    "{successfulCount: %d, failedCount: %d, totalCount: %d, successRate: %d}",
                    successfulCount, failedCount, totalCount, successRate);
        }

    }

    private class LogTask extends TimerTask {

        private final String threadName;
        private final long logIntervalSec;
        private final Timer timer = new Timer();

        private Stats prevStats;

        public LogTask(String threadName, long logInterval) {
            logIntervalSec = logInterval / 1000;
            this.threadName = !StringUtils.isBlank(threadName) ? threadName : THREAD_NAME_DEFAULT;

            timer.schedule(this, logInterval, logInterval);
        }

        @Override
        public void run() {
            Thread.currentThread().setName(threadName);
            calculateAndLog();
        }

        public void close() {
            timer.cancel();
        }

        private void calculateAndLog() {
            Stats stats = getStats();

            long throughput =
                    (stats.getTotalCount() - (prevStats != null ? prevStats.getTotalCount() : 0)) / logIntervalSec;

            LOGGER.info(
                    "total={}, successful={}, failed={}, success_rate={}%, througput={}/sec",
                    stats.getTotalCount(),
                    stats.getSuccessfulCount(),
                    stats.getFailedCount(),
                    stats.getSuccessRate(),
                    throughput);

            prevStats = stats;
        }

    }

}
