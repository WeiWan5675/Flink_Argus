package org.weiwan.argus.writer.hive;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.weiwan.argus.core.pub.api.BaseRichOutputFormat;
import org.weiwan.argus.core.pub.api.BaseWriter;
import org.weiwan.argus.core.pub.config.ArgusContext;
import org.weiwan.argus.core.pub.pojo.DataRecord;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/20 14:28
 * @Package: org.weiwan.argus.writer.hive
 * @ClassName: HiveWriter
 * @Description:
 **/
public class HiveWriter extends BaseWriter<DataRecord<Row>> {

    public HiveWriter(StreamExecutionEnvironment env, ArgusContext argusContext) {
        super(env, argusContext);
    }

    @Override
    public BaseRichOutputFormat getOutputFormat() {
        HiveOutputFormat hiveOutputFormat = new HiveOutputFormat(argusContext);
        return hiveOutputFormat;
    }
}
