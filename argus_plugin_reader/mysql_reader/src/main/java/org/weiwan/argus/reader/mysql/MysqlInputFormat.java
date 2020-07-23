package org.weiwan.argus.reader.mysql;

import org.apache.flink.types.Row;
import org.weiwan.argus.core.pub.api.JdbcInputFormat;
import org.weiwan.argus.core.pub.api.JobFormatState;
import org.weiwan.argus.core.pub.config.ArgusContext;
import org.weiwan.argus.core.pub.pojo.DataRecord;
import org.weiwan.argus.core.pub.api.BaseRichInputFormat;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/14 17:51
 * @Package: org.weiwan.argus.reader.mysql.input
 * @ClassName: MysqlInputFormat
 * @Description:
 **/
public class MysqlInputFormat extends JdbcInputFormat {

    public MysqlInputFormat(ArgusContext context) {
        super(context);
    }

}
