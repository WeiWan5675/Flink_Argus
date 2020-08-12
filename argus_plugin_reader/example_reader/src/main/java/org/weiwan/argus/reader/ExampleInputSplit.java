package org.weiwan.argus.reader;

import org.apache.flink.core.io.GenericInputSplit;

/**
 * @Author: xiaozhennan
 * @Date: 2020/8/12 16:39
 * @Package: org.weiwan.argus.reader.ExampleInputSplit
 * @ClassName: ExampleInputSplit
 * @Description:
 **/
public class ExampleInputSplit extends GenericInputSplit {
    /**
     * Creates a generic input split with the given split number.
     *
     * @param partitionNumber         The number of the split's partition.
     * @param totalNumberOfPartitions The total number of the splits (partitions).
     */
    public ExampleInputSplit(int partitionNumber, int totalNumberOfPartitions) {
        super(partitionNumber, totalNumberOfPartitions);
    }
}
