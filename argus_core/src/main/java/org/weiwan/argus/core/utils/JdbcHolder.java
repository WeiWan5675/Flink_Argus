package org.weiwan.argus.core.utils;

import java.sql.Connection;

/**
 * @Author: xiaozhennan
 * @Date: 2020/8/3 14:22
 * @Package: org.weiwan.argus.core.utils
 * @ClassName: JdbcHolder
 * @Description:
 **/
public interface JdbcHolder {

    public Connection getConnection();


}
