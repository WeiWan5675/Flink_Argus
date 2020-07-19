package org.weiwan.argus.reader.mysql;

import org.apache.flink.types.Row;
import org.weiwan.argus.core.pub.api.BaseReader;
import org.weiwan.argus.core.pub.api.BaseRichInputFormat;
import org.weiwan.argus.core.pub.pojo.DataRecord;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/14 17:49
 * @Package: org.weiwan.argus.reader.mysql
 * @ClassName: MysqlReader
 * @Description:
 **/
public class MysqlReader extends BaseReader<DataRecord<Row>> {

    @Override
    public BaseRichInputFormat getInputFormat() {

        return null;
    }


}
