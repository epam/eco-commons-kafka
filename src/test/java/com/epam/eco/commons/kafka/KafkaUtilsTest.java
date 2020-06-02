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
package com.epam.eco.commons.kafka;

import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Test;

import com.epam.eco.commons.kafka.KafkaUtils;
import com.epam.eco.commons.kafka.OffsetRange;

/**
 * @author Andrei_Tytsik
 */
public class KafkaUtilsTest {

    @Test
    public void testTopicPartitionIsParsedFromString() throws Exception {
        TopicPartition partition = KafkaUtils.parseTopicPartition("topic-0");
        Assert.assertNotNull(partition);
        Assert.assertEquals("topic", partition.topic());
        Assert.assertEquals(0, partition.partition());

        partition = KafkaUtils.parseTopicPartition("topic-189-ddd_df1-78");
        Assert.assertNotNull(partition);
        Assert.assertEquals("topic-189-ddd_df1", partition.topic());
        Assert.assertEquals(78, partition.partition());
    }

    @Test(expected=Exception.class)
    public void testTopicPartitionParseFailsOnNullString() throws Exception {
        KafkaUtils.parseTopicPartition(null);
    }

    @Test(expected=Exception.class)
    public void testTopicPartitionParseFailsOnBlankString() throws Exception {
        KafkaUtils.parseTopicPartition("   ");
    }

    @Test(expected=Exception.class)
    public void testTopicPartitionParseFailsOnInvalidString1() throws Exception {
        KafkaUtils.parseTopicPartition("topic");
    }

    @Test(expected=Exception.class)
    public void testTopicPartitionParseFailsOnInvalidString2() throws Exception {
        KafkaUtils.parseTopicPartition("topic_1");
    }

    @Test
    public void testConsumerLagCalculated() throws Exception {
        long lag = KafkaUtils.calculateConsumerLag(OffsetRange.with(10, 20, false), 10);
        Assert.assertEquals(10, lag);

        lag = KafkaUtils.calculateConsumerLag(OffsetRange.with(10, 20, true), 15);
        Assert.assertEquals(6, lag);

        lag = KafkaUtils.calculateConsumerLag(OffsetRange.with(10, 20, true), 0);
        Assert.assertEquals(11, lag);

        lag = KafkaUtils.calculateConsumerLag(OffsetRange.with(10, 20, false), 20);
        Assert.assertEquals(0, lag);

        lag = KafkaUtils.calculateConsumerLag(OffsetRange.with(10, 20, true), 20);
        Assert.assertEquals(1, lag);

        lag = KafkaUtils.calculateConsumerLag(OffsetRange.with(10, 10, false), 0);
        Assert.assertEquals(0, lag);

        lag = KafkaUtils.calculateConsumerLag(OffsetRange.with(10, 10, true), 0);
        Assert.assertEquals(1, lag);

        lag = KafkaUtils.calculateConsumerLag(OffsetRange.with(10, 10, true), 100);
        Assert.assertEquals(-1, lag);

        lag = KafkaUtils.calculateConsumerLag(OffsetRange.with(10, 20, false), 100);
        Assert.assertEquals(-1, lag);
    }

    @Test(expected=Exception.class)
    public void testConsumerLagCalculationFailsOnIvalidInput1() throws Exception {
        KafkaUtils.calculateConsumerLag(null, 10);
    }

    @Test(expected=Exception.class)
    public void testConsumerLagCalculationFailsOnIvalidInput2() throws Exception {
        KafkaUtils.calculateConsumerLag(OffsetRange.with(10, 20, false), -1);
    }

}
