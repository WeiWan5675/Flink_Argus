package org.weiwan.argus.core.pub.api;

import org.apache.commons.lang3.StringUtils;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/23 11:27
 * @Package: org.weiwan.argus.core.pub.api
 * @ClassName: SqlGenerator
 * @Description:
 **/
public class SqlGeneratorForMysql implements SqlGenerator {

    private SqlInfo sqlInfo;
    private static final String SPACE = " ";

    private static final String sql_basic = "select ${columns} from ${tableName} where 1 = 1 ${filterSql}";
    private static final String filterSql = " and ";

    private static final String splitSql = " and ${splitField} mod ${splitNum} = ${thisNum}";

    private static final String incrSql = " and ${incrField} BETWEEN ? AND ?";

    private static final String sqlOk = "select 1";

    public SqlGeneratorForMysql(SqlInfo sqlInfo) {
        this.sqlInfo = sqlInfo;
    }

    public SqlGeneratorForMysql() {

    }


    public static String generatorSql() {

        return sqlOk;
    }


    @Override
    public String generatorIncrSql(SqlInfo sqlInfo) {
        return null;
    }

    @Override
    public String generatorSql(SqlInfo sqlInfo) {
        return null;
    }

    public String generatorIncrSql() {

        StringBuffer filterSb = new StringBuffer(filterSql);


        String[] filters = sqlInfo.getFilters();

        for (int i = 0; i < filters.length; i++) {
            if (i < filters.length - 1) {
                filterSb.append(filters[i])
                        .append(" and ");
            } else {
                filterSb.append(filters[i] + "");
            }
        }

        StringBuffer columnSb = new StringBuffer("");
        String[] columns = sqlInfo.getColumns();
        for (int i = 0; i < columns.length; i++) {
            if (i < columns.length - 1) {
                columnSb.append(columns[i])
                        .append(",");
            } else {
                columnSb.append(columns[i]);
            }
        }

        String sql_tmp1 = sql_basic.replace("${columns}", columnSb.toString());
        String sql_tmp2 = sql_tmp1.replace("${filterSql}", filterSb.toString());
        String sql_tmp3 = sql_tmp2.replace("${tableName}", sqlInfo.getTableName());
        String sql_tmp4 = sql_tmp3;
        if (StringUtils.isNotEmpty(sqlInfo.getIncrField())) {
            //是增量任务,拼接增量SQL
            sql_tmp4 = sql_tmp3 + incrSql.replace("${incrField}", sqlInfo.getIncrField());
        }

        String sql_tmp5 = sql_tmp4;
        if (StringUtils.isNotEmpty(sqlInfo.getSplitField()) && sqlInfo.getSplitNum() != null && sqlInfo.getThisSplitNum() != null) {
            if (sqlInfo.getSplitNum() > 1) {
                sql_tmp5 = sql_tmp4 + splitSql
                        .replace("${splitField}", sqlInfo.getSplitField())
                        .replace("${splitNum}", String.valueOf(sqlInfo.getSplitNum()))
                        .replace("${thisNum}", String.valueOf(sqlInfo.getThisSplitNum()));
            }
        }
        return sql_tmp5;
    }


    public static void main(String[] args) {

        SqlInfo sqlInfo = SqlInfo.newBuilder()
                .lastOffset(924194190L)
                .thisSplitNum(0)
                .splitNum(3)
                .splitField("id")
                .incrField("update_time_dw")
                .columns(new String[]{"id", "broker_id", "dev_id"})
                .filters(new String[]{"id > 10000", "broker_id is not null and dev_id > 100"})
                .tableName("easylife_order")
                .build();

        SqlGeneratorForMysql sqlGenerator = new SqlGeneratorForMysql(sqlInfo);
        String sql = sqlGenerator.generatorIncrSql();
        System.out.println(sql);

    }

}
