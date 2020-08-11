package org.weiwan.argus.reader.mysql;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.weiwan.argus.core.pub.api.BaseReader;
import org.weiwan.argus.core.pub.input.BaseRichInputFormat;
import org.weiwan.argus.core.pub.config.ArgusContext;
import org.weiwan.argus.core.pub.pojo.DataRecord;
import org.weiwan.argus.core.pub.pojo.DataRow;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/14 17:49
 * @Package: org.weiwan.argus.reader.mysql
 * @ClassName: MysqlReader
 * @Description:
 **/
public class MysqlReader extends BaseReader<DataRecord<DataRow>> {


    public MysqlReader(StreamExecutionEnvironment env, ArgusContext argusContext) {
        super(env, argusContext);
    }

    @Override
    public BaseRichInputFormat getInputFormat(ArgusContext context) {
        MysqlInputFormat mysqlInputFormat = new MysqlInputFormat(context);
        return mysqlInputFormat;
    }

    @Override
    public void readRequire(ArgusContext argusContext) {

    }


    /**
     * <p> 方便某些自定义reader进行一些后处理工作
     *
     * @param stream  {@link DataStream<DataRecord>} 输入是这个
     * @param context
     * @return 输出也是这个
     */
    @Override
    protected DataStream<DataRecord<DataRow>> afterReading(DataStream<DataRecord<DataRow>> stream, ArgusContext context) {
        DataStream<DataRecord<DataRow>> map = stream.map(new MapFunction<DataRecord<DataRow>, DataRecord<DataRow>>() {
            @Override
            public DataRecord<DataRow> map(DataRecord<DataRow> value) throws Exception {
//                System.out.println("afterReading处理数据" + value.getData().toString());

                return value;
            }
        });
        return map;
    }
}
