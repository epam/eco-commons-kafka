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

/**
 * @author Andrei_Tytsik
 */
public enum TransactionState {

    EMPTY("Empty"),
    ONGOING("Ongoing"),
    PREPARE_COMMIT("PrepareCommit"),
    PREPARE_ABORT("PrepareAbort"),
    COMPLETE_COMMIT("CompleteCommit"),
    COMPLETE_ABORT("CompleteAbort"),
    DEAD("Dead"),
    PREPARE_EPOCH_FENCE("PrepareEpochFence");

    public final String name;

    TransactionState(String name) {
        this.name = name;
    }

    public static TransactionState fromScala(kafka.coordinator.transaction.TransactionState state) {
        Validate.notNull(state, "Transaction State is null");

        return forName(state.toString());
    }

    public static TransactionState forName(String name) {
        Validate.notBlank(name, "Name is blank");

        for (TransactionState state : values()) {
            if (state.name.equals(name)) {
                return state;
            }
        }

        throw new IllegalArgumentException("Unknown value: " + name);
    }

    /**
     * @deprecated use {@link #name} instead
     */
    @Deprecated
    public String id() {
        return name;
    }

    /**
     * @deprecated use {@link #forName(String)} instead
     */
    @Deprecated
    public static TransactionState getById(String id) {
        return forName(id);
    }

}