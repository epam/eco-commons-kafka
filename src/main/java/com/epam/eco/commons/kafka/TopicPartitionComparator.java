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

import java.util.Comparator;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.kafka.common.TopicPartition;

/**
 * @author Andrei_Tytsik
 */
public class TopicPartitionComparator implements Comparator<TopicPartition> {

    public static final TopicPartitionComparator INSTANCE = new TopicPartitionComparator();

    private TopicPartitionComparator() {
    }

    @Override
    public int compare(TopicPartition o1, TopicPartition o2) {
        int result = ObjectUtils.compare(o1.topic(), o2.topic());
        if (result == 0) {
            result = ObjectUtils.compare(o1.partition(), o2.partition());
        }
        return result;
    }

}
