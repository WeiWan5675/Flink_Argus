package org.weiwan.argus.reader.mysql.input;

import org.weiwan.argus.core.pub.api.BaseInputSpliter;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/14 17:52
 * @Package: org.weiwan.argus.reader.mysql.input
 * @ClassName: MysqlInputSpliter
 * @Description:
 **/
public class JdbcInputSpliter extends BaseInputSpliter {


    //多并行度时,通过自定义数据的分割模式,去处理
    private long minOffset;

    private long maxOffset;

    public JdbcInputSpliter(int partitionNumber, int totalNumberOfPartitions, long minOffset, long maxOffset) {
        super(partitionNumber, totalNumberOfPartitions);
        this.minOffset = minOffset;
        this.maxOffset = maxOffset;
    }


    public JdbcInputSpliter(int partitionNumber, int totalNumberOfPartitions) {
        super(partitionNumber, totalNumberOfPartitions);
    }


    public long getMinOffset() {
        return minOffset;
    }

    public void setMinOffset(long minOffset) {
        this.minOffset = minOffset;
    }

    public long getMaxOffset() {
        return maxOffset;
    }

    public void setMaxOffset(long maxOffset) {
        this.maxOffset = maxOffset;
    }
}
