package org.weiwan.argus.reader.mysql;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.weiwan.argus.core.pub.api.BaseReader;
import org.weiwan.argus.core.pub.api.BaseRichInputFormat;
import org.weiwan.argus.core.pub.config.ArgusContext;
import org.weiwan.argus.core.pub.pojo.DataRecord;
import org.weiwan.argus.reader.mysql.input.MysqlInputFormat;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/14 17:49
 * @Package: org.weiwan.argus.reader.mysql
 * @ClassName: MysqlReader
 * @Description:
 **/
public class MysqlReader extends BaseReader<DataRecord<Row>> {


    public MysqlReader(StreamExecutionEnvironment env, ArgusContext argusContext) {
        super(env, argusContext);
    }

    @Override
    public BaseRichInputFormat getInputFormat(ArgusContext context) {
        MysqlInputFormat mysqlInputFormat = new MysqlInputFormat();
        return mysqlInputFormat;
    }


}
