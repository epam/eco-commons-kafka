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
package com.epam.eco.commons.kafka;

import org.junit.Assert;
import org.junit.Test;

import com.epam.eco.commons.kafka.StatsLogger;
import com.epam.eco.commons.kafka.StatsLogger.Stats;

/**
 * @author Andrei_Tytsik
 */
public class StatsLoggerTest {

    @Test
    public void testStatsResolved() throws Exception {
        try (StatsLogger logger = new StatsLogger()) {
            Stats stats = logger.getStats();
            Assert.assertNotNull(stats);
            Assert.assertEquals(0, stats.getSuccessfulCount());
            Assert.assertEquals(0, stats.getFailedCount());
            Assert.assertEquals(0, stats.getTotalCount());
            Assert.assertEquals(0, stats.getSuccessRate());

            logger.report(50, 0);
            stats = logger.getStats();
            Assert.assertNotNull(stats);
            Assert.assertEquals(50, stats.getSuccessfulCount());
            Assert.assertEquals(0, stats.getFailedCount());
            Assert.assertEquals(50, stats.getTotalCount());
            Assert.assertEquals(100, stats.getSuccessRate());

            logger.report(0, 50);
            stats = logger.getStats();
            Assert.assertNotNull(stats);
            Assert.assertEquals(50, stats.getSuccessfulCount());
            Assert.assertEquals(50, stats.getFailedCount());
            Assert.assertEquals(100, stats.getTotalCount());
            Assert.assertEquals(50, stats.getSuccessRate());

            logger.incSuccessful();
            stats = logger.getStats();
            Assert.assertNotNull(stats);
            Assert.assertEquals(51, stats.getSuccessfulCount());
            Assert.assertEquals(50, stats.getFailedCount());
            Assert.assertEquals(101, stats.getTotalCount());

            logger.incFailed();
            stats = logger.getStats();
            Assert.assertNotNull(stats);
            Assert.assertEquals(51, stats.getSuccessfulCount());
            Assert.assertEquals(51, stats.getFailedCount());
            Assert.assertEquals(102, stats.getTotalCount());
        }
    }

}
