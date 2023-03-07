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
package com.epam.eco.commons.kafka.helpers;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.common.TopicPartition;

import com.epam.eco.commons.kafka.OffsetRange;

/**
 * @author Mikhail_Vershkov
 */

public class ConfluentUtils {

    public static Map<TopicPartition, Long> generateStartingOffsets(String topicName ,long[] startingOffsets) {
        return IntStream.range(0,startingOffsets.length)
                .boxed()
                .collect(Collectors.toMap(partition -> new TopicPartition(topicName, partition),
                        partition -> startingOffsets[partition]));
    }

    public static Map<TopicPartition, OffsetRange>  generateOffsetRanges(String topicName,
                                                                         long[] startingOffsets,
                                                                         OffsetRange[] offsetRanges) {
        return IntStream.range(0,startingOffsets.length)
                .boxed()
                .collect(Collectors.toMap( partition -> new TopicPartition(topicName, partition),
                        partition -> offsetRanges[partition]));
    }

    public static List<TopicPartition> generateTopicPartitions(String topicName, long[] startingOffsets) {
        return IntStream.range(0, startingOffsets.length)
                .mapToObj(ii->new TopicPartition(topicName,ii))
                .collect(Collectors.toList());
    }


}