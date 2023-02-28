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

    private final boolean smallestInclusive;
    private final boolean largestInclusive;
    private final long size;

    private final int hashCode;
    private final String toString;

    public OffsetRange(
            @JsonProperty("smallest") long smallest,
            @JsonProperty("smallestInclusive") boolean smallestInclusive,
            @JsonProperty("largest") long largest,
            @JsonProperty("largestInclusive") boolean largestInclusive) {
        Validate.isTrue(smallest >= 0, "Offset smallest value ("+smallest+") is invalid");
        Validate.isTrue(largest >= smallest, "Offset largest ("+ largest + ") value is invalid. (less then smallest (" + smallest + ") value)");

        this.smallest = smallest;
        this.largest = largest;
        this.largestInclusive = largestInclusive;
        this.smallestInclusive = smallestInclusive;
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
    public boolean isSmallestInclusive() {
        return smallestInclusive;
    }
    @JsonIgnore
    public long getSize() {
        return size;
    }
    public boolean contains(long value) {
        return  (smallest==largest && (smallestInclusive || largestInclusive) && smallest==value) ||
                ((smallestInclusive ? smallest <= value : smallest < value) &&
                (largestInclusive ? value <= largest : value < largest));
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
                Objects.equals(this.smallestInclusive, that.smallestInclusive) &&
                Objects.equals(this.largestInclusive, that.largestInclusive);
    }

    @Override
    public String toString() {
        return toString;
    }

    private final long calculateSize() {
        if(largest == smallest) {
            return (largestInclusive || smallestInclusive) ? 1 : 0;
        } else if(largest == smallest + 1) {
            if (largestInclusive && smallestInclusive) {
                return 2;
            } else if(largestInclusive || smallestInclusive) {
                return 1;
            } else {
                return 0;
            }
        }
        return largest + (largestInclusive ? 1 : 0) - (smallestInclusive ? 0 : 1) - smallest;
    }

    private final int calculateHashCode() {
        return Objects.hash(smallest, largest, smallestInclusive, largestInclusive);
    }

    private final String calculateToString() {
        return String.format("%s%d..%d%s(%d)", smallestInclusive ? "[" : "(", smallest, largest, largestInclusive ? "]" : ")", size);
    }

    public static OffsetRange with(long smallest, boolean smallestInclusive, long largest, boolean largestInclusive) {
        return new OffsetRange(smallest, smallestInclusive, largest, largestInclusive);
    }
    public static OffsetRange with(long smallest, long largest, boolean largestInclusive) {
        return new OffsetRange(smallest, true, largest, largestInclusive);
    }

}
