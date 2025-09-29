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
package com.epam.eco.commons.kafka.config;

import java.util.Properties;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.kafka.common.config.ConfigDef;

import kafka.server.KafkaConfig;

import static org.apache.kafka.server.config.KRaftConfigs.NODE_ID_CONFIG;
import static org.apache.kafka.server.config.KRaftConfigs.PROCESS_ROLES_CONFIG;

/**
 * @author Andrei_Tytsik
 */
public class BrokerConfigDef extends AbstractConfigDef {

    public static final BrokerConfigDef INSTANCE = new BrokerConfigDef();

    private BrokerConfigDef() {
        super(readDef());
    }

    private static ConfigDef readDef() {
        Properties props = new Properties();
        props.put(PROCESS_ROLES_CONFIG, "");
        props.put(NODE_ID_CONFIG, "0");
        KafkaConfig kafkaConfig = KafkaConfig.fromProps(props, false);

        try {
            return (ConfigDef)FieldUtils.readField(kafkaConfig, "definition", true);
        } catch (IllegalAccessException iae) {
            throw new RuntimeException(iae);
        }
    }

}
