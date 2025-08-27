package com.epam.eco.commons.kafka.helpers;

import java.util.List;
import java.util.stream.LongStream;

public class PartitionOffsets {
    private final long earliestOffset;
    private final long latestOffset;

    public PartitionOffsets(long earliestOffset, long latestOffset) {
        this.earliestOffset = earliestOffset;
        this.latestOffset = latestOffset;
    }

    public long getEarliestOffset() {
        return earliestOffset;
    }

    public long getLatestOffset() {
        return latestOffset;
    }

    public List<Long> offsets() {
        return LongStream.range(earliestOffset, latestOffset).boxed().toList();
    }
}
