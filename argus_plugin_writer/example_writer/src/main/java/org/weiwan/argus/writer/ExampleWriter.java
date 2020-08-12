package org.weiwan.argus.writer;

import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.weiwan.argus.core.pub.api.BaseWriter;
import org.weiwan.argus.core.pub.config.ArgusContext;
import org.weiwan.argus.core.pub.output.BaseRichOutputFormat;
import org.weiwan.argus.core.pub.pojo.DataField;
import org.weiwan.argus.core.pub.pojo.DataRecord;
import org.weiwan.argus.core.pub.pojo.DataRow;

import java.io.IOException;

/**
 * @Author: xiaozhennan
 * @Date: 2020/8/12 16:23
 * @Package: org.weiwan.argus.writer.ExampleWriter
 * @ClassName: ExampleWriter
 * @Description:
 **/
public class ExampleWriter extends BaseWriter<DataRecord<DataRow<DataField>>> {

    public static final Logger logger = LoggerFactory.getLogger(ExampleWriter.class);

    public ExampleWriter(StreamExecutionEnvironment env, ArgusContext argusContext) {
        super(env, argusContext);
    }

    @Override
    public BaseRichOutputFormat<DataRecord<DataRow<DataField>>> getOutputFormat() {
        logger.info("The method is called and returns the default Output Format");
        return new ExampleOutputFormat(argusContext);
    }

    /**
     * 为什么要在这里有这个方法呢,output是并行得,但是有些前置条件要再并行任务执行前处理,所以提供这个方法
     *
     * @param context
     */
    @Override
    public void writeRequire(ArgusContext context) {
        logger.info("verify Necessary Parameters Before Write");
    }


}
