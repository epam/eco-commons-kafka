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

import java.util.Objects;

import org.apache.commons.lang3.Validate;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author Andrei_Tytsik
 */
public class OffsetRange {

    private final long smallest;
    private final long largest;
    private final boolean largestInclusive;
    private final long size;

    private final int hashCode;
    private final String toString;

    public OffsetRange(
            @JsonProperty("smallest") long smallest,
            @JsonProperty("largest") long largest,
            @JsonProperty("largestInclusive") boolean largestInclusive) {
        Validate.isTrue(smallest >= 0, "Offset smallest value is invalid");
        Validate.isTrue(largest >= smallest, "Offset largest value is invalid");

        this.smallest = smallest;
        this.largest = largest;
        this.largestInclusive = largestInclusive;
        this.size = calculateSize();

        hashCode = calculateHashCode();
        toString = calculateToString();
    }

    public long getSmallest() {
        return smallest;
    }
    public long getLargest() {
        return largest;
    }
    public boolean isLargestInclusive() {
        return largestInclusive;
    }
    @JsonIgnore
    public long getSize() {
        return size;
    }
    public boolean contains(long value) {
        return smallest <= value && (largestInclusive ? value <= largest : value < largest);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        if (this == obj) {
            return true;
        }
        OffsetRange that = (OffsetRange)obj;
        return
                Objects.equals(this.smallest, that.smallest) &&
                Objects.equals(this.largest, that.largest) &&
                Objects.equals(this.largestInclusive, that.largestInclusive);
    }

    @Override
    public String toString() {
        return toString;
    }

    private long calculateSize() {
        return largest + (largestInclusive ? 1 : 0) - smallest;
    }

    private int calculateHashCode() {
        return Objects.hash(smallest, largest, largestInclusive);
    }

    private String calculateToString() {
        return String.format("[%d..%d%s(%d)", smallest, largest, largestInclusive ? "]" : ")", size);
    }

    public static OffsetRange with(long smallest, long largest, boolean largestInclusive) {
        return new OffsetRange(smallest, largest, largestInclusive);
    }

}
