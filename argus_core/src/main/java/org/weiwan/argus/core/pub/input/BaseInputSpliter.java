package org.weiwan.argus.core.pub.input;

import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/14 17:53
 * @Package: org.weiwan.argus.pub.api
 * @ClassName: BaseInputSpliter
 * @Description:
 **/
public abstract class BaseInputSpliter extends GenericInputSplit {

    //处理模式,批处理,流处理
    private String mod;

    //父类方法,这些必须要传递
    public BaseInputSpliter(int partitionNumber, int totalNumberOfPartitions) {
        super(partitionNumber, totalNumberOfPartitions);
    }


    public String getMod() {
        return mod;
    }

    public void setMod(String mod) {
        this.mod = mod;
    }
}
