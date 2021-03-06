package org.weiwan.argus.core.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @Author: xiaozhennan
 * @Date: 2020/8/3 14:35
 * @Package: org.weiwan.argus.core.utils
 * @ClassName: MysqlJdbcHolder
 * @Description:
 **/
public class MysqlJdbcHolder implements JdbcHolder {

    private JdbcInfo jdbcInfo;
    private String jdbcUrl;
    private String username;
    private String password;
    private String database;
    private Connection connection;


    public MysqlJdbcHolder(JdbcInfo jdbcInfo) throws SQLException {
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
