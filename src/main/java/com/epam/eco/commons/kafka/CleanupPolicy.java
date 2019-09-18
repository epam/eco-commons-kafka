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

import org.apache.kafka.common.config.TopicConfig;

/**
 * @author Andrei_Tytsik
 */
public enum CleanupPolicy {

    COMPACT(TopicConfig.CLEANUP_POLICY_COMPACT),
    DELETE(TopicConfig.CLEANUP_POLICY_DELETE);

    public final String name;

    CleanupPolicy(String name) {
        this.name = name;
    }

}
