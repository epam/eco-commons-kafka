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

import java.util.Collection;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

/**
 * @author Andrei_Tytsik
 */
public final class BeginningOffsetInitializer implements OffsetInitializer {

    public static final BeginningOffsetInitializer INSTANCE = new BeginningOffsetInitializer();

    private BeginningOffsetInitializer() {
    }

    @Override
    public void init(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        consumer.seekToBeginning(partitions);
    }

}
