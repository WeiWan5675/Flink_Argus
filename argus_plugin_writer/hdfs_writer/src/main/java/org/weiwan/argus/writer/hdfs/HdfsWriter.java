package org.weiwan.argus.writer.hdfs;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.weiwan.argus.core.pub.api.BaseWriter;
import org.weiwan.argus.core.pub.config.ArgusContext;
import org.weiwan.argus.core.pub.output.BaseRichOutputFormat;
import org.weiwan.argus.core.pub.output.hdfs.HdfsOutputFormat;
import org.weiwan.argus.core.pub.output.hdfs.HdfsOutputFormatV2;
import org.weiwan.argus.core.pub.pojo.DataField;
import org.weiwan.argus.core.pub.pojo.DataRecord;
import org.weiwan.argus.core.pub.pojo.DataRow;

/**
 * @Author: xiaozhennan
 * @Date: 2020/8/7 15:38
 * @Package: org.weiwan.argus.writer.hdfs
 * @ClassName: HdfsWriter
 * @Description:
 **/
public class HdfsWriter extends BaseWriter<DataRecord<DataRow<DataField>>> {

    public HdfsWriter(StreamExecutionEnvironment env, ArgusContext argusContext) {
        super(env, argusContext);
    }

    @Override
    public BaseRichOutputFormat<DataRecord<DataRow<DataField>>> getOutputFormat(ArgusContext argusContext) {
        HdfsOutputFormatV2 hdfsOutputFormat = new HdfsOutputFormatV2(argusContext);
        return hdfsOutputFormat;
    }

    /**
     * 为什么要在这里有这个方法呢,output是并行得,但是有些前置条件要再并行任务执行前处理,所以提供这个方法
     *
     * @param context
     */
    @Override
    public void writeRequire(ArgusContext context) {

    }
}
