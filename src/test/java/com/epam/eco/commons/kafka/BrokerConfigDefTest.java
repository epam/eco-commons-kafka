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

import org.apache.commons.lang3.Validate;
import org.apache.kafka.common.config.ConfigDef.ConfigKey;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.junit.jupiter.api.Test;

import com.epam.eco.commons.kafka.config.BrokerConfigDef;

import static org.apache.kafka.server.config.KRaftConfigs.NODE_ID_CONFIG;

/**
 * @author Andrei_Tytsik
 */
public class BrokerConfigDefTest {

    @Test
    public void testKeyIsResolved() {
        ConfigKey key = BrokerConfigDef.INSTANCE.key(NODE_ID_CONFIG);
        Validate.notNull(key);
    }

    @Test
    public void testDocIsResolved() {
        String doc = BrokerConfigDef.INSTANCE.doc(NODE_ID_CONFIG);
        Validate.notNull(doc);
    }

    @Test
    public void testImportanceIsResolved() {
        Importance importance = BrokerConfigDef.INSTANCE.importance(NODE_ID_CONFIG);
        Validate.notNull(importance);
    }

    @Test
    public void testTypeIsResolved() {
        Type type = BrokerConfigDef.INSTANCE.type(NODE_ID_CONFIG);
        Validate.notNull(type);
    }

}
