package org.weiwan.argus.plugin.mongo.input;

import org.weiwan.argus.core.pub.api.BaseInputSpliter;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/22 14:52
 * @Package: org.weiwan.argus.plugin.mongo.input
 * @ClassName: MongoInputSpliter
 * @Description:
 **/
public class MongoInputSpliter extends BaseInputSpliter {
    public MongoInputSpliter(int partitionNumber, int totalNumberOfPartitions) {
        super(partitionNumber, totalNumberOfPartitions);
    }
}
