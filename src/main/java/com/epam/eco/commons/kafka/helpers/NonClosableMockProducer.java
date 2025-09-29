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

import java.time.Duration;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Avoids setting 'close' flag. So, object could be reused for several tests
 * @author Aliaksei_Valyaev
 */
public class NonClosableMockProducer<K, V> extends MockProducer<K, V> {

    public NonClosableMockProducer(
            Cluster cluster,
            boolean autoComplete,
            Partitioner partitioner,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer
    ) {
        super(cluster, autoComplete, partitioner, keySerializer, valueSerializer);
    }

    public NonClosableMockProducer(
            boolean autoComplete,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer
    ) {
        super(Cluster.empty(), autoComplete, null, keySerializer, valueSerializer);
    }

    public NonClosableMockProducer(
            boolean autoComplete,
            Partitioner partitioner,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer
    ) {
        super(autoComplete, partitioner, keySerializer, valueSerializer);
    }

    public NonClosableMockProducer() {
    }

    @Override
    public void close(Duration timeout) {
        if (this.closeException != null) {
            throw this.closeException;
        }
        // skip setting 'close'
    }
}
