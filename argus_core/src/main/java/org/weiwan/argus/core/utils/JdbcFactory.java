package org.weiwan.argus.core.utils;

import org.apache.commons.lang3.StringUtils;

import java.sql.DriverManager;

/**
 * @Author: xiaozhennan
 * @Date: 2020/8/3 14:18
 * @Package: org.weiwan.argus.core.utils
 * @ClassName: JdbcHandlerFactory
 * @Description:
 **/
public class JdbcFactory {

    private static JdbcHolder jdbcHolder;

    public static final String JDBC_HIVE_DRIVE = "org.apache.hive.jdbc.HiveDriver";
    public static final String JDBC_MYSQL_DRIVE = "com.mysql.jdbc.Driver";

    public static JdbcHolder newInstance(JdbcInfo jdbcInfo) throws Exception {
        String drive = jdbcInfo.getDriveClassName();
        try {
            if (StringUtils.isEmpty(drive)) {
                //为空
                throw new RuntimeException("jdbc drive class name cant is null");
            } else {
                Class.forName(drive);
            }
        } catch (Exception e) {
            throw e;
        }


        switch (drive) {
            case JDBC_HIVE_DRIVE:
                jdbcHolder = new HiveJbdcHolder(jdbcInfo);
                break;
            case JDBC_MYSQL_DRIVE:
                jdbcHolder = new MysqlJdbcHolder(jdbcInfo);
                break;

        }
        return jdbcHolder;
    }

}
