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

import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.epam.eco.commons.kafka.util.TestObjectMapperSingleton;

/**
 * @author Andrei_Tytsik
 */
public class OffsetRangeTest {

    @Test
    public void testRangeHasExpectedValues() throws Exception {
        OffsetRange range = OffsetRange.with(0, false,0, false);
        Assert.assertNotNull(range);
        Assert.assertFalse(range.isLargestInclusive());
        Assert.assertFalse(range.isLargestInclusive());
        Assert.assertEquals(0, range.getSmallest());
        Assert.assertEquals(0, range.getLargest());
        Assert.assertEquals(0, range.getSize());
        Assert.assertFalse(range.contains(-1));
        Assert.assertFalse(range.contains(0));
        Assert.assertFalse(range.contains(1));

        range = OffsetRange.with(0, true,0, false);
        Assert.assertNotNull(range);
        Assert.assertTrue(range.isSmallestInclusive());
        Assert.assertFalse(range.isLargestInclusive());
        Assert.assertEquals(0, range.getSmallest());
        Assert.assertEquals(0, range.getLargest());
        Assert.assertEquals(1, range.getSize());
        Assert.assertFalse(range.contains(-1));
        Assert.assertTrue(range.contains(0));
        Assert.assertFalse(range.contains(1));

        range = OffsetRange.with(0, false,0, true);
        Assert.assertNotNull(range);
        Assert.assertFalse(range.isSmallestInclusive());
        Assert.assertTrue(range.isLargestInclusive());
        Assert.assertEquals(0, range.getSmallest());
        Assert.assertEquals(0, range.getLargest());
        Assert.assertEquals(1, range.getSize());
        Assert.assertFalse(range.contains(-1));
        Assert.assertTrue(range.contains(0));
        Assert.assertFalse(range.contains(1));


        range = OffsetRange.with(0, true, 0, true);
        Assert.assertNotNull(range);
        Assert.assertTrue(range.isSmallestInclusive());
        Assert.assertTrue(range.isLargestInclusive());
        Assert.assertEquals(0, range.getSmallest());
        Assert.assertEquals(0, range.getLargest());
        Assert.assertEquals(1, range.getSize());
        Assert.assertFalse(range.contains(-1));
        Assert.assertTrue(range.contains(0));
        Assert.assertFalse(range.contains(1));




        range = OffsetRange.with(0, false, 1, false);
        Assert.assertNotNull(range);
        Assert.assertFalse(range.isSmallestInclusive());
        Assert.assertFalse(range.isLargestInclusive());
        Assert.assertEquals(0, range.getSmallest());
        Assert.assertEquals(1, range.getLargest());
        Assert.assertEquals(0, range.getSize());
        Assert.assertFalse(range.contains(-1));
        Assert.assertFalse(range.contains(0));
        Assert.assertFalse(range.contains(1));


        range = OffsetRange.with(0, true, 1, false);
        Assert.assertNotNull(range);
        Assert.assertTrue(range.isSmallestInclusive());
        Assert.assertFalse(range.isLargestInclusive());
        Assert.assertEquals(0, range.getSmallest());
        Assert.assertEquals(1, range.getLargest());
        Assert.assertEquals(1, range.getSize());
        Assert.assertFalse(range.contains(-1));
        Assert.assertTrue(range.contains(0));
        Assert.assertFalse(range.contains(1));

        range = OffsetRange.with(0, false, 1, true);
        Assert.assertNotNull(range);
        Assert.assertFalse(range.isSmallestInclusive());
        Assert.assertTrue(range.isLargestInclusive());
        Assert.assertEquals(0, range.getSmallest());
        Assert.assertEquals(1, range.getLargest());
        Assert.assertEquals(1, range.getSize());
        Assert.assertFalse(range.contains(-1));
        Assert.assertFalse(range.contains(0));
        Assert.assertTrue(range.contains(1));

        range = OffsetRange.with(0, true, 1, true);
        Assert.assertNotNull(range);
        Assert.assertTrue(range.isSmallestInclusive());
        Assert.assertTrue(range.isLargestInclusive());
        Assert.assertEquals(0, range.getSmallest());
        Assert.assertEquals(1, range.getLargest());
        Assert.assertEquals(2, range.getSize());
        Assert.assertFalse(range.contains(-1));
        Assert.assertTrue(range.contains(0));
        Assert.assertTrue(range.contains(1));




        range = OffsetRange.with(1, false,1, false);
        Assert.assertNotNull(range);
        Assert.assertFalse(range.isSmallestInclusive());
        Assert.assertFalse(range.isLargestInclusive());
        Assert.assertEquals(1, range.getSmallest());
        Assert.assertEquals(1, range.getLargest());
        Assert.assertEquals(0, range.getSize());
        Assert.assertFalse(range.contains(-1));
        Assert.assertFalse(range.contains(0));
        Assert.assertFalse(range.contains(1));
        Assert.assertFalse(range.contains(2));

        range = OffsetRange.with(1, true,1, false);
        Assert.assertNotNull(range);
        Assert.assertTrue(range.isSmallestInclusive());
        Assert.assertFalse(range.isLargestInclusive());
        Assert.assertEquals(1, range.getSmallest());
        Assert.assertEquals(1, range.getLargest());
        Assert.assertEquals(1, range.getSize());
        Assert.assertFalse(range.contains(-1));
        Assert.assertFalse(range.contains(0));
        Assert.assertTrue(range.contains(1));
        Assert.assertFalse(range.contains(2));

        range = OffsetRange.with(1, false,1, true);
        Assert.assertNotNull(range);
        Assert.assertFalse(range.isSmallestInclusive());
        Assert.assertTrue(range.isLargestInclusive());
        Assert.assertEquals(1, range.getSmallest());
        Assert.assertEquals(1, range.getLargest());
        Assert.assertEquals(1, range.getSize());
        Assert.assertFalse(range.contains(-1));
        Assert.assertFalse(range.contains(0));
        Assert.assertTrue(range.contains(1));
        Assert.assertFalse(range.contains(2));

        range = OffsetRange.with(1, true,1, true);
        Assert.assertNotNull(range);
        Assert.assertTrue(range.isLargestInclusive());
        Assert.assertTrue(range.isLargestInclusive());
        Assert.assertEquals(1, range.getSmallest());
        Assert.assertEquals(1, range.getLargest());
        Assert.assertEquals(1, range.getSize());
        Assert.assertFalse(range.contains(-1));
        Assert.assertFalse(range.contains(0));
        Assert.assertTrue(range.contains(1));
        Assert.assertFalse(range.contains(2));


        range = OffsetRange.with(0, false, 10, false);
        Assert.assertNotNull(range);
        Assert.assertEquals(0, range.getSmallest());
        Assert.assertEquals(10, range.getLargest());
        Assert.assertEquals(9, range.getSize());
        Assert.assertFalse(range.contains(-1));
        Assert.assertFalse(range.contains(0));
        Assert.assertTrue(range.contains(1));
        Assert.assertTrue(range.contains(5));
        Assert.assertFalse(range.contains(10));
        Assert.assertFalse(range.contains(11));

        range = OffsetRange.with(0, true, 10, false);
        Assert.assertNotNull(range);
        Assert.assertEquals(0, range.getSmallest());
        Assert.assertEquals(10, range.getLargest());
        Assert.assertEquals(10, range.getSize());
        Assert.assertFalse(range.contains(-1));
        Assert.assertTrue(range.contains(0));
        Assert.assertTrue(range.contains(5));
        Assert.assertFalse(range.contains(10));
        Assert.assertFalse(range.contains(11));

        range = OffsetRange.with(0, false, 10, true);
        Assert.assertNotNull(range);
        Assert.assertEquals(0, range.getSmallest());
        Assert.assertEquals(10, range.getLargest());
        Assert.assertEquals(10, range.getSize());
        Assert.assertFalse(range.contains(-1));
        Assert.assertFalse(range.contains(0));
        Assert.assertTrue(range.contains(1));
        Assert.assertTrue(range.contains(5));
        Assert.assertTrue(range.contains(10));
        Assert.assertFalse(range.contains(11));

        range = OffsetRange.with(0, true,10, true);
        Assert.assertNotNull(range);
        Assert.assertEquals(0, range.getSmallest());
        Assert.assertEquals(10, range.getLargest());
        Assert.assertEquals(11, range.getSize());
        Assert.assertFalse(range.contains(-1));
        Assert.assertTrue(range.contains(0));
        Assert.assertTrue(range.contains(5));
        Assert.assertTrue(range.contains(10));
        Assert.assertFalse(range.contains(11));
    }

    @Test(expected=Exception.class)
    public void testCreaetionFailsOnInvalidArguments1() throws Exception {
        OffsetRange.with(-1, 1, true);
    }

    @Test(expected=Exception.class)
    public void testCreaetionFailsOnInvalidArguments2() throws Exception {
        OffsetRange.with(1, 0, true);
    }

    @Test
    public void testSerializedToJsonAndBack() throws Exception {
        OffsetRange origin = OffsetRange.with(0, 1, true);

        ObjectMapper mapper = TestObjectMapperSingleton.INSTANCE;

        String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(origin);
        Assert.assertNotNull(json);

        OffsetRange deserialized = mapper.readValue(
                json,
                OffsetRange.class);
        Assert.assertNotNull(deserialized);
        Assert.assertEquals(origin, deserialized);
    }

}
