package org.weiwan.argus.writer.hive;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.weiwan.argus.core.pub.output.BaseRichOutputFormat;
import org.weiwan.argus.core.pub.api.BaseWriter;
import org.weiwan.argus.core.pub.config.ArgusContext;
import org.weiwan.argus.core.pub.pojo.DataRecord;
import org.weiwan.argus.core.pub.pojo.DataRow;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/20 14:28
 * @Package: org.weiwan.argus.writer.hive
 * @ClassName: HiveWriter
 * @Description:
 **/
public class HiveWriter extends BaseWriter<DataRecord<DataRow>> {

    public HiveWriter(StreamExecutionEnvironment env, ArgusContext argusContext) {
        super(env, argusContext);
    }

    @Override
    public BaseRichOutputFormat getOutputFormat(ArgusContext argusContext) {
        HiveOutputFormat hiveOutputFormat = new HiveOutputFormat(argusContext);
        return hiveOutputFormat;
    }

    @Override
    public void writeRequire(ArgusContext context) {

    }

}
