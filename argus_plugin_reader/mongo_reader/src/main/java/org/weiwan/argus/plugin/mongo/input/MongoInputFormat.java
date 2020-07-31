package org.weiwan.argus.plugin.mongo.input;

import org.apache.flink.types.Row;
import org.weiwan.argus.core.pub.input.BaseRichInputFormat;
import org.weiwan.argus.core.pub.pojo.JobFormatState;
import org.weiwan.argus.core.pub.pojo.DataRecord;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/22 14:51
 * @Package: org.weiwan.argus.plugin.mongo.input
 * @ClassName: MongoInputFormat
 * @Description:
 **/
public class MongoInputFormat extends BaseRichInputFormat<DataRecord<Row>, MongoInputSpliter> {

    /**
     * 打开InputFormat,根据split读取数据
     *
     * @param split 当前处理的分区InputSplit
     */
    @Override
    public void openInput(MongoInputSpliter split) {

    }

    /**
     * 根据minNumSplits决定如何划分分区
     *
     * @param minNumSplits 最小分区数
     * @return 分区数组
     */
    @Override
    public MongoInputSpliter[] getInputSpliter(int minNumSplits) {
        return new MongoInputSpliter[0];
    }

    /**
     * 返回一条记录
     * 当数据处理结束后,需要手动调用{@link BaseRichInputFormat#isComplete} }
     *
     * @param reuse
     * @return 数据
     */
    @Override
    public DataRecord<Row> nextRecordInternal(DataRecord<Row> reuse) {
        return null;
    }

    /**
     * 关闭Input,释放资源
     */
    @Override
    public void closeInput() {

    }

    /**
     * 快照前允许特殊处理
     *
     * @param formatState
     */
    @Override
    public void snapshot(JobFormatState formatState) {
    }
}
