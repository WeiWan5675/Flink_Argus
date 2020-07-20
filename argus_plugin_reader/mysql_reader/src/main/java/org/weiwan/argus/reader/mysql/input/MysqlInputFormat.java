package org.weiwan.argus.reader.mysql.input;

import org.apache.flink.types.Row;
import org.weiwan.argus.core.pub.pojo.DataRecord;
import org.weiwan.argus.core.pub.api.BaseRichInputFormat;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/14 17:51
 * @Package: org.weiwan.argus.reader.mysql.input
 * @ClassName: MysqlInputFormat
 * @Description:
 **/
public class MysqlInputFormat extends BaseRichInputFormat<DataRecord<Row>, MysqlInputSpliter> {


}
