package org.weiwan.argus.reader.oplog.input;

import org.weiwan.argus.core.pub.input.BaseInputSpliter;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/22 15:43
 * @Package: org.weiwan.argus.core.pub.api
 * @ClassName: JdbcInputSpliter
 * @Description:
 **/
public class OplogInputSpliter extends BaseInputSpliter {

    public OplogInputSpliter(int partitionNumber, int totalNumberOfPartitions) {
        super(partitionNumber, totalNumberOfPartitions);
    }

}
