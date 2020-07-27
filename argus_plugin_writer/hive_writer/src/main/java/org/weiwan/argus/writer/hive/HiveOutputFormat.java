package org.weiwan.argus.writer.hive;

import org.apache.flink.types.Row;
import org.weiwan.argus.core.pub.api.BaseRichOutputFormat;
import org.weiwan.argus.core.pub.config.ArgusContext;
import org.weiwan.argus.core.pub.pojo.DataRecord;

import java.io.IOException;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/20 17:03
 * @Package: org.weiwan.argus.writer.hive
 * @ClassName: HiveOutputFormat
 * @Description:
 **/
public class HiveOutputFormat extends BaseRichOutputFormat<DataRecord<Row>> {


    public HiveOutputFormat(ArgusContext argusContext) {
        super(argusContext);
    }


    @Override
    public void writeRecord(DataRecord<Row> record) throws IOException {
        System.out.println(record);
        i++;
    }
}
