/*******************************************************************************
 *  Copyright 2023 EPAM Systems
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

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author Andrei_Tytsik
 */
public class KafkaUtilsTest {

    @Test
    public void testTopicPartitionIsParsedFromString() {
        TopicPartition partition = KafkaUtils.parseTopicPartition("topic-0");
        Assertions.assertNotNull(partition);
        Assertions.assertEquals("topic", partition.topic());
        Assertions.assertEquals(0, partition.partition());

        partition = KafkaUtils.parseTopicPartition("topic-189-ddd_df1-78");
        Assertions.assertNotNull(partition);
        Assertions.assertEquals("topic-189-ddd_df1", partition.topic());
        Assertions.assertEquals(78, partition.partition());
    }

    @Test
    public void testTopicPartitionParseFailsOnNullString() {
        assertThrows(Exception.class, () -> KafkaUtils.parseTopicPartition(null));
    }

    @Test
    public void testTopicPartitionParseFailsOnBlankString() {
        assertThrows(Exception.class, () -> KafkaUtils.parseTopicPartition("   "));
    }

    @Test
    public void testTopicPartitionParseFailsOnInvalidString1() {
        assertThrows(Exception.class, () -> KafkaUtils.parseTopicPartition("topic"));
    }

    @Test
    public void testTopicPartitionParseFailsOnInvalidString2() {
        assertThrows(Exception.class, () -> KafkaUtils.parseTopicPartition("topic_1"));
    }

    @Test
    public void testConsumerLagCalculated() {
        long lag = KafkaUtils.calculateConsumerLag(OffsetRange.with(10, 20, false), 10);
        Assertions.assertEquals(10, lag);

        lag = KafkaUtils.calculateConsumerLag(OffsetRange.with(10, 20, true), 15);
        Assertions.assertEquals(6, lag);

        lag = KafkaUtils.calculateConsumerLag(OffsetRange.with(10, 20, true), 0);
        Assertions.assertEquals(11, lag);

        lag = KafkaUtils.calculateConsumerLag(OffsetRange.with(10, 20, false), 20);
        Assertions.assertEquals(0, lag);

        lag = KafkaUtils.calculateConsumerLag(OffsetRange.with(10, 20, true), 20);
        Assertions.assertEquals(1, lag);

        lag = KafkaUtils.calculateConsumerLag(OffsetRange.with(10,false,10, false), 0);
        Assertions.assertEquals(0, lag);

        lag = KafkaUtils.calculateConsumerLag(OffsetRange.with(10, 10, true), 0);
        Assertions.assertEquals(1, lag);

        lag = KafkaUtils.calculateConsumerLag(OffsetRange.with(10, 10, true), 100);
        Assertions.assertEquals(-1, lag);

        lag = KafkaUtils.calculateConsumerLag(OffsetRange.with(10, 20, false), 100);
        Assertions.assertEquals(-1, lag);
    }

    @Test
    public void testConsumerLagCalculationFailsOnIvalidInput1() {
        assertThrows(Exception.class, () -> KafkaUtils.calculateConsumerLag(null, 10));
    }

    @Test
    public void testConsumerLagCalculationFailsOnIvalidInput2() {
        assertThrows(Exception.class, () -> KafkaUtils.calculateConsumerLag(OffsetRange.with(10, 20, false), -1));
    }

}
