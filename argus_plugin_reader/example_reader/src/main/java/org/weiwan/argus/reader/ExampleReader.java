package org.weiwan.argus.reader;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.weiwan.argus.core.pub.api.BaseReader;
import org.weiwan.argus.core.pub.config.ArgusContext;
import org.weiwan.argus.core.pub.input.BaseRichInputFormat;
import org.weiwan.argus.core.pub.pojo.DataField;
import org.weiwan.argus.core.pub.pojo.DataRecord;
import org.weiwan.argus.core.pub.pojo.DataRow;

/**
 * @Author: xiaozhennan
 * @Date: 2020/8/12 16:36
 * @Package: org.weiwan.argus.reader.ExampleReader
 * @ClassName: ExampleReader
 * @Description:
 **/
public class ExampleReader extends BaseReader<DataRecord<DataRow<DataField>>> {

    public static final Logger logger = LoggerFactory.getLogger(ExampleReader.class);

    public ExampleReader(StreamExecutionEnvironment env, ArgusContext argusContext) {
        super(env, argusContext);
    }

    @Override
    public BaseRichInputFormat getInputFormat(ArgusContext context) {
        logger.info("this Method Returns InputFormat");
        return new ExampleInputFormat(context);
    }

    @Override
    public void readRequire(ArgusContext argusContext) {
        logger.info("do Some Checks Before Reading");
    }
}
