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

import java.util.HashMap;
import java.util.Properties;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.kafka.common.config.ConfigDef;

import kafka.log.LogConfig;

/**
 * @author Andrei_Tytsik
 */
public class TopicConfigDef extends AbstractConfigDef {

    public static final TopicConfigDef INSTANCE = new TopicConfigDef();

    private TopicConfigDef() {
        super(readDef());
    }

    private static ConfigDef readDef() {
        LogConfig logConfig = LogConfig.fromProps(new HashMap<>(), new Properties());
        try {
            return (ConfigDef)FieldUtils.readField(logConfig, "definition", true);
        } catch (IllegalAccessException iae) {
            throw new RuntimeException(iae);
        }
    }

}
