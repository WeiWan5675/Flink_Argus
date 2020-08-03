package org.weiwan.argus.core.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @Author: xiaozhennan
 * @Date: 2020/8/3 14:42
 * @Package: org.weiwan.argus.core.utils
 * @ClassName: HiveJbdcHolder
 * @Description:
 **/
public class HiveJbdcHolder implements JdbcHolder {
    private JdbcInfo jdbcInfo;
    private String jdbcUrl;
    private String username;
    private String password;
    private String database;
    private Connection connection;

    public HiveJbdcHolder(JdbcInfo jdbcInfo) throws SQLException {
        this.jdbcInfo = jdbcInfo;
        this.jdbcUrl = jdbcInfo.getJdbcUrl();
        this.username = jdbcInfo.getUsername();
        this.password = jdbcInfo.getPassword();
        this.database = jdbcInfo.getDatabase();
        this.connection = DriverManager.getConnection(jdbcUrl, username, password);
    }

    @Override
    public Connection getConnection() {
        return connection;
    }
}
