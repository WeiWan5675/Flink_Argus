package org.weiwan.argus.reader.mysql;

import org.weiwan.argus.core.pub.input.jdbc.JdbcInputFormat;
import org.weiwan.argus.core.pub.input.jdbc.JdbcInputSpliter;
import org.weiwan.argus.core.pub.config.ArgusContext;
import org.weiwan.argus.core.pub.input.jdbc.SqlGenerator;
import org.weiwan.argus.core.pub.input.jdbc.SqlGeneratorForMysql;

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

    @Override
    public SqlGenerator getSqlGenerator(JdbcInputSpliter split) {
        //打开数据源
        return new SqlGeneratorForMysql();
    }

}
