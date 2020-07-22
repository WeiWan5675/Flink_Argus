package org.weiwan.argus.reader.mysql.input;

import org.apache.flink.types.Row;
import org.weiwan.argus.core.pub.api.JobFormatState;
import org.weiwan.argus.core.pub.config.ArgusContext;
import org.weiwan.argus.core.pub.pojo.DataRecord;
import org.weiwan.argus.core.pub.api.BaseRichInputFormat;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/14 17:51
 * @Package: org.weiwan.argus.reader.mysql.input
 * @ClassName: MysqlInputFormat
 * @Description:
 **/
public class MysqlInputFormat extends BaseRichInputFormat<DataRecord<Row>, JdbcInputSpliter> {


    private List<String> result;
    private Iterator<String> dataIterator;

    public MysqlInputFormat(ArgusContext context) {
        super(context);
    }


    /**
     * 打开InputFormat,根据split读取数据
     *
     * @param split 当前处理的分区InputSplit
     */
    @Override
    public void openInput(JdbcInputSpliter split) {
        //读取数据
        //模拟数据源
        result = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            result.add("TestValue" + i);
        }
        dataIterator = result.iterator();
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
            spliters[i] = jdbcInputSpliter;
        }
        return spliters;
    }

    /**
     * 返回一条记录
     *
     * @param reuse
     * @return 数据
     */
    @Override
    public DataRecord<Row> nextRecordInternal(DataRecord<Row> reuse) {
        System.out.println("子类处理下一条记录");
        DataRecord<Row> rowDataRecord = null;
        if (dataIterator.hasNext()) {
            String next = dataIterator.next();
            Row row = new Row(1000);
            row.setField(0, next);
            reuse = new DataRecord<>(row);
        } else {
            isComplete(true);
        }
        return reuse;
    }

    /**
     * 关闭Input,释放资源
     */
    @Override
    public void closeInput() {
        result = null;
    }

    /**
     * 快照前允许特殊处理
     *
     * @param formatState
     */
    @Override
    public void snapshot(JobFormatState formatState) {
        System.out.println("子类实现快照方法:" + formatState.toString());
    }
}
