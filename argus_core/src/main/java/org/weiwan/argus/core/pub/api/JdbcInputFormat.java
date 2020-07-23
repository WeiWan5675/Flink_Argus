package org.weiwan.argus.core.pub.api;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.weiwan.argus.core.pub.config.ArgusContext;
import org.weiwan.argus.core.pub.pojo.DataRecord;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.*;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/22 15:42
 * @Package: org.weiwan.argus.core.pub.api
 * @ClassName: JdbcInputFormat
 * @Description:
 **/
public abstract class JdbcInputFormat extends BaseRichInputFormat<DataRecord<Row>, JdbcInputSpliter> {


    private static final String KEY_READER_TYPE = "reader.type";
    private static final String KEY_READER_NAME = "reader.name";
    private static final String KEY_READER_CLASS = "reader.class";
    private static final String KEY_READER_JDBC_URL = "reader.jdbc.url";
    private static final String KEY_READER_JDBC_DRIVE_CLASS = "reader.jdbc.drive";
    private static final String KEY_READER_JDBC_USERNAME = "reader.jdbc.username";
    private static final String KEY_READER_JDBC_PASSWROD = "reader.jdbc.password";
    private static final String KEY_READER_JDBC_SCHEMA = "reader.jdbc.schema";
    private static final String KEY_READER_TABLENAME = "reader.tableName";
    private static final String KEY_READER_BATCHSIZE = "reader.batchSize";
    private static final String KEY_READER_QUERY_TIMEOUT = "reader.queryTimeout";
    private static final String KEY_READER_SPLIT_FIELD = "reader.splitField";
    private static final String KEY_READER_SQL_FILTER = "reader.sql.filter";
    private static final String KEY_READER_SQL_COLUMNS = "reader.sql.columns";
    private static final String KEY_READER_SQL_CUSTOMSQL = "reader.sql.customSql";
    private static final String KEY_READER_INCR_INCRFIELD = "reader.increment.incrField";
    private static final String KEY_READER_INCR_LASTOFFSET = "reader.increment.lastOffset";
    private static final String KEY_READER_INCR_ENABLE_POLLING = "reader.increment.enablePolling";
    private static final String KEY_READER_INCR_POLL_INTERVAL = "reader.increment.pollingInterval";

    protected String driveClassName;
    protected String jdbcUrl;
    protected String username;
    protected String password;

    protected String tableName;
    protected int batchSize;
    protected int queryTimeout;
    protected boolean isPolling;
    protected Long pollingInterval;
    protected String[] tables;
    protected String[] columns;
    protected String[] filters;
    protected String incrField;
    protected String splitField;

    protected Connection dbConn;
    protected PreparedStatement statement;
    protected ResultSet resultSet;
    protected ResultSetMetaData tableMetaData;
    protected int columnCount;
    protected String lastOffset;
    protected SqlGenerator sqlGenerator;

    public JdbcInputFormat(ArgusContext context) {
        super(context);
    }

    @Override
    public void configure(Configuration parameters) {
        this.username = readerConfig.getStringVal(KEY_READER_JDBC_USERNAME);
        this.password = readerConfig.getStringVal(KEY_READER_JDBC_PASSWROD);
        this.driveClassName = readerConfig.getStringVal(KEY_READER_JDBC_DRIVE_CLASS);
        this.tableName = readerConfig.getStringVal(KEY_READER_TABLENAME);
        this.batchSize = readerConfig.getIntVal(KEY_READER_BATCHSIZE, 1000);
        this.queryTimeout = readerConfig.getIntVal(KEY_READER_QUERY_TIMEOUT, 60);
        this.jdbcUrl = readerConfig.getStringVal(KEY_READER_JDBC_URL);
        this.pollingInterval = readerConfig.getLongVal(KEY_READER_INCR_POLL_INTERVAL, 10000L);
        this.isPolling = readerConfig.getBooleanVal(KEY_READER_INCR_ENABLE_POLLING, false);
        this.columns = readerConfig.getStringVal(KEY_READER_SQL_COLUMNS, "1").split(",");
        this.filters = readerConfig.getStringVal(KEY_READER_SQL_FILTER, "1=1").split(",");
        this.incrField = readerConfig.getStringVal(KEY_READER_INCR_INCRFIELD);
        this.splitField = readerConfig.getStringVal(KEY_READER_SPLIT_FIELD);
        this.lastOffset = readerConfig.getStringVal(KEY_READER_INCR_LASTOFFSET);
    }

    /**
     * 打开InputFormat,根据split读取数据
     *
     * @param split 当前处理的分区InputSplit
     */
    @Override
    public void openInput(JdbcInputSpliter split) {
        try {
            Class.forName(driveClassName);
            this.dbConn = DriverManager.getConnection(jdbcUrl, username, password);
            //打开数据源
            SqlInfo sqlInfo = SqlInfo.newBuilder()
                    .tableName(tableName)
                    .columns(columns)
                    .filters(filters)
                    .incrField(incrField)
                    .splitField(splitField)
                    .splitNum(split.getTotalNumberOfSplits())
                    .thisSplitNum(split.getSplitNumber())
                    .build();
            sqlGenerator = new SqlGenerator(sqlInfo);
            //构建sql


            String sql = sqlGenerator.generatorIncrSql();
            statement = dbConn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
            System.out.println("sql: " + sql);
            //获得最大最小 max min
            if (isPolling) {
                statement.setFetchSize(batchSize);
                statement.setQueryTimeout(queryTimeout);
                statement.setString(1, "2020-07-01 00:00:00");
                statement.setString(2, "2020-07-05 00:00:00");
            } else {
                //获取offset Sql 直接访问数据库获得 根据IncrField 字段
                statement.setFetchSize(batchSize);
                statement.setQueryTimeout(queryTimeout);
                statement.setString(1, "2020-07-01 00:00:00");
                statement.setString(2, "2020-07-05 00:00:00");
                resultSet = statement.executeQuery();
                tableMetaData = resultSet.getMetaData();
                columnCount = tableMetaData.getColumnCount();
            }


            //初始化调用下next,不然result会报错
            boolean next = resultSet.next();
            if (!next) {
                isComplete(true);
            }

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

    }

    /**
     * 根据minNumSplits决定如何划分分区
     *
     * @param minNumSplits 最小分区数
     * @return 分区数组
     */
    @Override
    public JdbcInputSpliter[] getInputSpliter(int minNumSplits) {
        JdbcInputSpliter[] spliters = new JdbcInputSpliter[minNumSplits];
        for (int i = 0; i < minNumSplits; i++) {
            JdbcInputSpliter jdbcInputSpliter = new JdbcInputSpliter(i, minNumSplits);
            String stringVal = readerConfig.getStringVal(KEY_READER_INCR_LASTOFFSET); //从启动参数中获取offset
//            jdbcInputSpliter.setUserLastOffser(Long.valueOf(stringVal));
            spliters[i] = jdbcInputSpliter;
        }
        return spliters;
    }


    /**
     * 返回一条记录
     * 当数据处理结束后,需要手动调用{@link BaseRichInputFormat#isComplete} }
     *
     * @param row
     * @return 数据
     */
    @Override
    public DataRecord<Row> nextRecordInternal(DataRecord<Row> row) {
        DataRecord<Row> rowDataRecord = new DataRecord();
        try {
            Row currentRow = new Row(columnCount);
            rowDataRecord.setData(currentRow);
            if (!isComplete()) {
                for (int i = 0; i < columnCount; i++) {

                    Object object = resultSet.getObject(i + 1);
                    String columnName = tableMetaData.getColumnName(i + 1);
                    currentRow.setField(i, object);
                }
            }

            boolean next = resultSet.next();
            //如果没有数据了,就isComplete()
            if (!next) {
                isComplete(true);
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return rowDataRecord;
    }


    /**
     * 关闭Input,释放资源
     */
    @Override
    public void closeInput() {

    }

    /**
     * 快照前允许特殊处理
     *
     * @param formatState
     */
    @Override
    public void snapshot(JobFormatState formatState) {

    }


    /**
     *
     *
     *
     * 1. 支持的模式
     *      1.1 全量拉取 最大最小值 直接抽取
     *      1.2 指定offset 增量拉取 从指定offset开始拉取
     *      1.3 轮询拉取 根据起始offset 按指定间隔 将最新的数据抽取出去
     *          两种模式: 1. 按数值列新增抽取  2. 按update_time列 抽取更新的数据
     *
     * 2. {@link JdbcInputFormat} 要做的事情
     *      2.1 创建数据库连接
     *      2.2 执行SQL
     *      2.3 如果是轮询模式,需要处理动态sql问题
     *      2.4 多表暂时不考虑,单独提供InputFormat
     *3. 继承JdbcInputFormat的子类做的事情
     *
     *      3.1 打开数据源
     *      3.2 获取下一条数据
     *      3.3 拼接SQL,抽象SQL返回分段
     *
     */


}
