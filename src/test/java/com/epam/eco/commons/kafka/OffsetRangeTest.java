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

import com.fasterxml.jackson.databind.ObjectMapper;

import com.epam.eco.commons.kafka.util.TestObjectMapperSingleton;

/**
 * @author Andrei_Tytsik
 */
public class OffsetRangeTest {

    @Test
    public void testRangeHasExpectedValues() throws Exception {
        OffsetRange range = OffsetRange.with(0, false,0, false);
        Assertions.assertNotNull(range);
        Assertions.assertFalse(range.isLargestInclusive());
        Assertions.assertFalse(range.isLargestInclusive());
        Assertions.assertEquals(0, range.getSmallest());
        Assertions.assertEquals(0, range.getLargest());
        Assertions.assertEquals(0, range.getSize());
        Assertions.assertFalse(range.contains(-1));
        Assertions.assertFalse(range.contains(0));
        Assertions.assertFalse(range.contains(1));

        range = OffsetRange.with(0, true,0, false);
        Assertions.assertNotNull(range);
        Assertions.assertTrue(range.isSmallestInclusive());
        Assertions.assertFalse(range.isLargestInclusive());
        Assertions.assertEquals(0, range.getSmallest());
        Assertions.assertEquals(0, range.getLargest());
        Assertions.assertEquals(1, range.getSize());
        Assertions.assertFalse(range.contains(-1));
        Assertions.assertTrue(range.contains(0));
        Assertions.assertFalse(range.contains(1));

        range = OffsetRange.with(0, false,0, true);
        Assertions.assertNotNull(range);
        Assertions.assertFalse(range.isSmallestInclusive());
        Assertions.assertTrue(range.isLargestInclusive());
        Assertions.assertEquals(0, range.getSmallest());
        Assertions.assertEquals(0, range.getLargest());
        Assertions.assertEquals(1, range.getSize());
        Assertions.assertFalse(range.contains(-1));
        Assertions.assertTrue(range.contains(0));
        Assertions.assertFalse(range.contains(1));


        range = OffsetRange.with(0, true, 0, true);
        Assertions.assertNotNull(range);
        Assertions.assertTrue(range.isSmallestInclusive());
        Assertions.assertTrue(range.isLargestInclusive());
        Assertions.assertEquals(0, range.getSmallest());
        Assertions.assertEquals(0, range.getLargest());
        Assertions.assertEquals(1, range.getSize());
        Assertions.assertFalse(range.contains(-1));
        Assertions.assertTrue(range.contains(0));
        Assertions.assertFalse(range.contains(1));




        range = OffsetRange.with(0, false, 1, false);
        Assertions.assertNotNull(range);
        Assertions.assertFalse(range.isSmallestInclusive());
        Assertions.assertFalse(range.isLargestInclusive());
        Assertions.assertEquals(0, range.getSmallest());
        Assertions.assertEquals(1, range.getLargest());
        Assertions.assertEquals(0, range.getSize());
        Assertions.assertFalse(range.contains(-1));
        Assertions.assertFalse(range.contains(0));
        Assertions.assertFalse(range.contains(1));


        range = OffsetRange.with(0, true, 1, false);
        Assertions.assertNotNull(range);
        Assertions.assertTrue(range.isSmallestInclusive());
        Assertions.assertFalse(range.isLargestInclusive());
        Assertions.assertEquals(0, range.getSmallest());
        Assertions.assertEquals(1, range.getLargest());
        Assertions.assertEquals(1, range.getSize());
        Assertions.assertFalse(range.contains(-1));
        Assertions.assertTrue(range.contains(0));
        Assertions.assertFalse(range.contains(1));

        range = OffsetRange.with(0, false, 1, true);
        Assertions.assertNotNull(range);
        Assertions.assertFalse(range.isSmallestInclusive());
        Assertions.assertTrue(range.isLargestInclusive());
        Assertions.assertEquals(0, range.getSmallest());
        Assertions.assertEquals(1, range.getLargest());
        Assertions.assertEquals(1, range.getSize());
        Assertions.assertFalse(range.contains(-1));
        Assertions.assertFalse(range.contains(0));
        Assertions.assertTrue(range.contains(1));

        range = OffsetRange.with(0, true, 1, true);
        Assertions.assertNotNull(range);
        Assertions.assertTrue(range.isSmallestInclusive());
        Assertions.assertTrue(range.isLargestInclusive());
        Assertions.assertEquals(0, range.getSmallest());
        Assertions.assertEquals(1, range.getLargest());
        Assertions.assertEquals(2, range.getSize());
        Assertions.assertFalse(range.contains(-1));
        Assertions.assertTrue(range.contains(0));
        Assertions.assertTrue(range.contains(1));




        range = OffsetRange.with(1, false,1, false);
        Assertions.assertNotNull(range);
        Assertions.assertFalse(range.isSmallestInclusive());
        Assertions.assertFalse(range.isLargestInclusive());
        Assertions.assertEquals(1, range.getSmallest());
        Assertions.assertEquals(1, range.getLargest());
        Assertions.assertEquals(0, range.getSize());
        Assertions.assertFalse(range.contains(-1));
        Assertions.assertFalse(range.contains(0));
        Assertions.assertFalse(range.contains(1));
        Assertions.assertFalse(range.contains(2));

        range = OffsetRange.with(1, true,1, false);
        Assertions.assertNotNull(range);
        Assertions.assertTrue(range.isSmallestInclusive());
        Assertions.assertFalse(range.isLargestInclusive());
        Assertions.assertEquals(1, range.getSmallest());
        Assertions.assertEquals(1, range.getLargest());
        Assertions.assertEquals(1, range.getSize());
        Assertions.assertFalse(range.contains(-1));
        Assertions.assertFalse(range.contains(0));
        Assertions.assertTrue(range.contains(1));
        Assertions.assertFalse(range.contains(2));

        range = OffsetRange.with(1, false,1, true);
        Assertions.assertNotNull(range);
        Assertions.assertFalse(range.isSmallestInclusive());
        Assertions.assertTrue(range.isLargestInclusive());
        Assertions.assertEquals(1, range.getSmallest());
        Assertions.assertEquals(1, range.getLargest());
        Assertions.assertEquals(1, range.getSize());
        Assertions.assertFalse(range.contains(-1));
        Assertions.assertFalse(range.contains(0));
        Assertions.assertTrue(range.contains(1));
        Assertions.assertFalse(range.contains(2));

        range = OffsetRange.with(1, true,1, true);
        Assertions.assertNotNull(range);
        Assertions.assertTrue(range.isLargestInclusive());
        Assertions.assertTrue(range.isLargestInclusive());
        Assertions.assertEquals(1, range.getSmallest());
        Assertions.assertEquals(1, range.getLargest());
        Assertions.assertEquals(1, range.getSize());
        Assertions.assertFalse(range.contains(-1));
        Assertions.assertFalse(range.contains(0));
        Assertions.assertTrue(range.contains(1));
        Assertions.assertFalse(range.contains(2));


        range = OffsetRange.with(0, false, 10, false);
        Assertions.assertNotNull(range);
        Assertions.assertEquals(0, range.getSmallest());
        Assertions.assertEquals(10, range.getLargest());
        Assertions.assertEquals(9, range.getSize());
        Assertions.assertFalse(range.contains(-1));
        Assertions.assertFalse(range.contains(0));
        Assertions.assertTrue(range.contains(1));
        Assertions.assertTrue(range.contains(5));
        Assertions.assertFalse(range.contains(10));
        Assertions.assertFalse(range.contains(11));

        range = OffsetRange.with(0, true, 10, false);
        Assertions.assertNotNull(range);
        Assertions.assertEquals(0, range.getSmallest());
        Assertions.assertEquals(10, range.getLargest());
        Assertions.assertEquals(10, range.getSize());
        Assertions.assertFalse(range.contains(-1));
        Assertions.assertTrue(range.contains(0));
        Assertions.assertTrue(range.contains(5));
        Assertions.assertFalse(range.contains(10));
        Assertions.assertFalse(range.contains(11));

        range = OffsetRange.with(0, false, 10, true);
        Assertions.assertNotNull(range);
        Assertions.assertEquals(0, range.getSmallest());
        Assertions.assertEquals(10, range.getLargest());
        Assertions.assertEquals(10, range.getSize());
        Assertions.assertFalse(range.contains(-1));
        Assertions.assertFalse(range.contains(0));
        Assertions.assertTrue(range.contains(1));
        Assertions.assertTrue(range.contains(5));
        Assertions.assertTrue(range.contains(10));
        Assertions.assertFalse(range.contains(11));

        range = OffsetRange.with(0, true,10, true);
        Assertions.assertNotNull(range);
        Assertions.assertEquals(0, range.getSmallest());
        Assertions.assertEquals(10, range.getLargest());
        Assertions.assertEquals(11, range.getSize());
        Assertions.assertFalse(range.contains(-1));
        Assertions.assertTrue(range.contains(0));
        Assertions.assertTrue(range.contains(5));
        Assertions.assertTrue(range.contains(10));
        Assertions.assertFalse(range.contains(11));
    }

    @Test
    public void testCreaetionFailsOnInvalidArguments1() throws Exception {
        Assertions.assertThrows(Exception.class, () -> OffsetRange.with(-1, 1, true));
    }

    @Test
    public void testCreaetionFailsOnInvalidArguments2() throws Exception {
        Assertions.assertThrows(Exception.class, () -> OffsetRange.with(1, 0, true));
    }

    @Test
    public void testSerializedToJsonAndBack() throws Exception {
        OffsetRange origin = OffsetRange.with(0, 1, true);

        ObjectMapper mapper = TestObjectMapperSingleton.INSTANCE;

        String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(origin);
        Assertions.assertNotNull(json);

        OffsetRange deserialized = mapper.readValue(
                json,
                OffsetRange.class);
        Assertions.assertNotNull(deserialized);
        Assertions.assertEquals(origin, deserialized);
    }

}
