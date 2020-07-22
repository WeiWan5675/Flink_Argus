package org.weiwan.argus.core.pub.api;

import org.apache.flink.types.Row;
import org.weiwan.argus.core.pub.pojo.DataRecord;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/22 15:42
 * @Package: org.weiwan.argus.core.pub.api
 * @ClassName: JdbcInputFormat
 * @Description:
 **/
public abstract class JdbcInputFormat extends BaseRichInputFormat<DataRecord<Row>, JdbcInputSpliter> {

    protected String driveClassName;
    protected String jdbcUrl;
    protected String username;
    protected String password;

    protected String tableName;
    protected String[] tables;

    protected DataSource dataSource;
    protected Connection dbConn;
    protected PreparedStatement preparedStatement;

    /**
     * 打开InputFormat,根据split读取数据
     *
     * @param split 当前处理的分区InputSplit
     */
    @Override
    public void openInput(JdbcInputSpliter split) {
        //打开数据源
    }

    /**
     * 根据minNumSplits决定如何划分分区
     *
     * @param minNumSplits 最小分区数
     * @return 分区数组
     */
    @Override
    public JdbcInputSpliter[] getInputSpliter(int minNumSplits) {
        return new JdbcInputSpliter[0];
    }

    /**
     * 返回一条记录
     * 当数据处理结束后,需要手动调用{@link BaseRichInputFormat#isComplete} }
     *
     * @param reuse
     * @return 数据
     */
    @Override
    public DataRecord<Row> nextRecordInternal(DataRecord<Row> reuse) {
        return null;
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
