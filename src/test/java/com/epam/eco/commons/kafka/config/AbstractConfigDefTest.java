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
package com.epam.eco.commons.kafka.config;

import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Andrei_Tytsik
 */
public class AbstractConfigDefTest {

    @Test
    public void testDefaultValueResolved() throws Exception {
        AbstractConfigDef configDef = BrokerConfigDef.INSTANCE;

        Assert.assertEquals(null, configDef.defaultValue("inter.broker.listener.name"));
        Assert.assertEquals("null", configDef.defaultValueAsString("inter.broker.listener.name"));

        Assert.assertEquals(true, configDef.defaultValue("auto.leader.rebalance.enable"));
        Assert.assertEquals("true", configDef.defaultValueAsString("auto.leader.rebalance.enable"));

        Assert.assertEquals(Collections.emptyList(), configDef.defaultValue("ssl.cipher.suites"));
        Assert.assertEquals("\"\"", configDef.defaultValueAsString("ssl.cipher.suites"));

        Assert.assertEquals(Collections.singletonList("delete"), configDef.defaultValue("log.cleanup.policy"));
        Assert.assertEquals("delete", configDef.defaultValueAsString("log.cleanup.policy"));
    }

    @Test
    public void testDefaultValueCompared() throws Exception {
        AbstractConfigDef configDef = BrokerConfigDef.INSTANCE;

        Assert.assertTrue(configDef.isDefaultValue("inter.broker.listener.name", null));
        Assert.assertTrue(configDef.isDefaultValue("inter.broker.listener.name", "null"));

        Assert.assertTrue(configDef.isDefaultValue("auto.leader.rebalance.enable", true));
        Assert.assertTrue(configDef.isDefaultValue("auto.leader.rebalance.enable", "true"));

        Assert.assertTrue(configDef.isDefaultValue("ssl.cipher.suites", Collections.emptyList()));
        Assert.assertTrue(configDef.isDefaultValue("ssl.cipher.suites", "\"\""));

        Assert.assertTrue(configDef.isDefaultValue("log.cleanup.policy", Collections.singletonList("delete")));
        Assert.assertTrue(configDef.isDefaultValue("log.cleanup.policy", "delete"));
    }

}
