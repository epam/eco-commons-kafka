/*
 * Copyright 2020 EPAM Systems
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

/**
 * @author Andrei_Tytsik
 */
public enum Acks {

    ALL("all"), ONE("1"), NONE("0");

    public final String name;

    Acks(String name) {
        this.name = name;
    }

    public static Acks forName(String name) {
        Validate.notBlank(name, "Name is blank");

        for (Acks acks : values()) {
            if (acks.name.equals(name)) {
                return acks;
            }
        }

        throw new IllegalArgumentException("Unknown value: " + name);
    }

}
