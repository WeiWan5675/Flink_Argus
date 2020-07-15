package org.weiwan.argus.core.pub.api;

import org.apache.flink.core.io.InputSplit;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/14 17:53
 * @Package: org.weiwan.argus.pub.api
 * @ClassName: BaseInputSpliter
 * @Description:
 **/
public abstract class BaseInputSpliter implements InputSplit {
    /**
     * Returns the number of this input split.
     *
     * @return the number of this input split
     */
    @Override
    public int getSplitNumber() {
        return 0;
    }

    public abstract InputSpliter getInputSpliter();
}
