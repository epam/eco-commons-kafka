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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.epam.eco.commons.kafka.StatsLogger.Stats;

/**
 * @author Andrei_Tytsik
 */
public class StatsLoggerTest {

    @Test
    public void testStatsResolved() {
        try (StatsLogger logger = new StatsLogger()) {
            Stats stats = logger.getStats();
            Assertions.assertNotNull(stats);
            Assertions.assertEquals(0, stats.getSuccessfulCount());
            Assertions.assertEquals(0, stats.getFailedCount());
            Assertions.assertEquals(0, stats.getTotalCount());
            Assertions.assertEquals(0, stats.getSuccessRate());

            logger.report(50, 0);
            stats = logger.getStats();
            Assertions.assertNotNull(stats);
            Assertions.assertEquals(50, stats.getSuccessfulCount());
            Assertions.assertEquals(0, stats.getFailedCount());
            Assertions.assertEquals(50, stats.getTotalCount());
            Assertions.assertEquals(100, stats.getSuccessRate());

            logger.report(0, 50);
            stats = logger.getStats();
            Assertions.assertNotNull(stats);
            Assertions.assertEquals(50, stats.getSuccessfulCount());
            Assertions.assertEquals(50, stats.getFailedCount());
            Assertions.assertEquals(100, stats.getTotalCount());
            Assertions.assertEquals(50, stats.getSuccessRate());

            logger.incSuccessful();
            stats = logger.getStats();
            Assertions.assertNotNull(stats);
            Assertions.assertEquals(51, stats.getSuccessfulCount());
            Assertions.assertEquals(50, stats.getFailedCount());
            Assertions.assertEquals(101, stats.getTotalCount());

            logger.incFailed();
            stats = logger.getStats();
            Assertions.assertNotNull(stats);
            Assertions.assertEquals(51, stats.getSuccessfulCount());
            Assertions.assertEquals(51, stats.getFailedCount());
            Assertions.assertEquals(102, stats.getTotalCount());
        }
    }

}
