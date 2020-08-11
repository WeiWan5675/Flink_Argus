package org.weiwan.argus.core.start;

import org.weiwan.argus.common.options.Option;
import org.weiwan.argus.common.options.OptionField;

/**
 * @Author: xiaozhennan
 * @Date: 2020/8/11 13:46
 * @Package: org.weiwan.argus.core.start.MysqlToHiveOptions
 * @ClassName: MysqlToHiveOptions
 * @Description:
 **/
@Option("MysqlToHiveOptions")
public class MysqlToHiveOptions extends StartOptions {

    @OptionField(oweKeys = "targetTable", required = true)
    private String targetTable;

    @OptionField(oweKeys = "sourceTable", required = true)
    private String sourceTable;

    @OptionField(oweKeys = "autoCreateTable", hasArg = false)
    private boolean autoCreateTable;

    @OptionField(oweKeys = "lastOffset")
    private String lastOffset;

    @OptionField(oweKeys = "splitNum")
    private String splitNum;

    @OptionField(oweKeys = "splitField")
    private String splitField;

    @OptionField(oweKeys = "dbConf")
    private String dbConf;

    @OptionField(oweKeys = "username")
    private String username;

    @OptionField(oweKeys = "password")
    private String password;


    @OptionField(oweKeys = "jdbcUrl")
    private String jdbcUrl;

    @OptionField(oweKeys = "database")
    private String database;

    @OptionField(oweKeys = "queryColumns")
    private String queryColumns;

    @OptionField(oweKeys = "customSql")
    private String customSql;

    @OptionField(oweKeys = "filterSql")
    private String filterSql;

    @OptionField(oweKeys = "queryTimeout")
    private String queryTimeout;

    @OptionField(oweKeys = "batchSize")
    private Integer batchSize;


}
