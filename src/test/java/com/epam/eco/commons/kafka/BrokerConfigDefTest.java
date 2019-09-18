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

import org.apache.commons.lang3.Validate;
import org.apache.kafka.common.config.ConfigDef.ConfigKey;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.junit.Test;

import com.epam.eco.commons.kafka.config.BrokerConfigDef;

/**
 * @author Andrei_Tytsik
 */
public class BrokerConfigDefTest {

    @Test
    public void testKeyIsResolved() throws Exception {
        ConfigKey key = BrokerConfigDef.INSTANCE.key("zookeeper.connect");
        Validate.notNull(key);
    }

    @Test
    public void testDocIsResolved() throws Exception {
        String doc = BrokerConfigDef.INSTANCE.doc("zookeeper.connect");
        Validate.notNull(doc);
    }

    @Test
    public void testImportanceIsResolved() throws Exception {
        Importance importance = BrokerConfigDef.INSTANCE.importance("zookeeper.connect");
        Validate.notNull(importance);
    }

    @Test
    public void testTypeIsResolved() throws Exception {
        Type type = BrokerConfigDef.INSTANCE.type("zookeeper.connect");
        Validate.notNull(type);
    }

}
