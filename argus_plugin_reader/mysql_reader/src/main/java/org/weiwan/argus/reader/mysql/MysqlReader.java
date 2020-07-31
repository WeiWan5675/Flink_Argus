package org.weiwan.argus.reader.mysql;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.weiwan.argus.core.pub.api.BaseReader;
import org.weiwan.argus.core.pub.input.BaseRichInputFormat;
import org.weiwan.argus.core.pub.config.ArgusContext;
import org.weiwan.argus.core.pub.pojo.DataRecord;

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
        MysqlInputFormat mysqlInputFormat = new MysqlInputFormat(context);
        return mysqlInputFormat;
    }


    /**
     * <p> 方便某些自定义reader进行一些后处理工作
     *
     * @param stream  {@link DataStream<DataRecord>} 输入是这个
     * @param context
     * @return 输出也是这个
     */
    @Override
    protected DataStream<DataRecord<Row>> afterReading(DataStream<DataRecord<Row>> stream, ArgusContext context) {
        DataStream<DataRecord<Row>> map = stream.map(new MapFunction<DataRecord<Row>, DataRecord<Row>>() {
            @Override
            public DataRecord<Row> map(DataRecord<Row> value) throws Exception {
                System.out.println("afterReading处理数据" + value.getData().toString());

                return value;
            }
        });
        return map;
    }
}
