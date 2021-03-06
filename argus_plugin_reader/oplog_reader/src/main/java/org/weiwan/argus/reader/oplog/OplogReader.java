package org.weiwan.argus.reader.oplog;

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
 * @Author: lsl
 * @Date: 2020/8/21 11:12
 * @Description:
 **/
public class OplogReader extends BaseReader<DataRecord<DataRow<DataField>>> {

    public static final Logger logger = LoggerFactory.getLogger(OplogReader.class);


    public OplogReader(StreamExecutionEnvironment env, ArgusContext argusContext) {
        super(env, argusContext);
    }

    @Override
    public BaseRichInputFormat getInputFormat(ArgusContext context) {
        return new OplogInputFormat(context);
    }

    @Override
    public void readRequire(ArgusContext argusContext) {

    }
}
