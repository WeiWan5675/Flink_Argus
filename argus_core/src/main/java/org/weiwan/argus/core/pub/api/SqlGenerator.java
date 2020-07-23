package org.weiwan.argus.core.pub.api;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/23 18:19
 * @Package: org.weiwan.argus.core.pub.api
 * @ClassName: SqlGenerator
 * @Description:
 **/
public interface SqlGenerator {

    public String generatorIncrSql(SqlInfo sqlInfo);

    public String generatorSql(SqlInfo sqlInfo);

    public String generatorIncrSql();
}
