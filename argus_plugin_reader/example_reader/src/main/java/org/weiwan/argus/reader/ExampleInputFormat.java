package org.weiwan.argus.reader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.weiwan.argus.common.utils.DateUtils;
import org.weiwan.argus.core.pub.config.ArgusContext;
import org.weiwan.argus.core.pub.input.BaseRichInputFormat;
import org.weiwan.argus.core.pub.enums.ColumnType;
import org.weiwan.argus.core.pub.pojo.DataField;
import org.weiwan.argus.core.pub.pojo.DataRecord;
import org.weiwan.argus.core.pub.pojo.DataRow;
import org.weiwan.argus.core.pub.pojo.JobFormatState;

import java.util.Date;

/**
 * @Author: xiaozhennan
 * @Date: 2020/8/12 16:38
 * @Package: org.weiwan.argus.reader.ExampleInputFormat
 * @ClassName: ExampleInputFormat
 * @Description:
 **/
public class ExampleInputFormat extends BaseRichInputFormat<DataRecord<DataRow<DataField>>, ExampleInputSplit> {

    public static final Logger logger = LoggerFactory.getLogger(ExampleInputFormat.class);

    int endIndex = 1000;
    int currentIndex = 0;

    public ExampleInputFormat(ArgusContext context) {
        super(context);
    }

    /**
     * 打开InputFormat,根据split读取数据
     *
     * @param split 当前处理的分区InputSplit
     */
    @Override
    public void openInput(ExampleInputSplit split) {
        this.endIndex = readerConfig.getIntVal("writer.example.endIndex", 1000);
        logger.info("open this InputFormat");
    }

    /**
     * 根据minNumSplits决定如何划分分区
     *
     * @param minNumSplits 最小分区数
     * @return 分区数组
     */
    @Override
    public ExampleInputSplit[] getInputSpliter(int minNumSplits) {

        ExampleInputSplit[] splits = new ExampleInputSplit[minNumSplits];
        for (int i = 0; i < minNumSplits; i++){
            ExampleInputSplit exampleInputSplit = new ExampleInputSplit(i, minNumSplits);
            splits[i] = exampleInputSplit;
        }
        return splits;
    }

    /**
     * 返回一条记录
     * 当数据处理结束后,需要手动调用{@link BaseRichInputFormat#isComplete} }
     * 如果不想使用isComplete 需要重写{@link BaseRichInputFormat#reachedEnd()}
     *
     * @param reuse
     * @return 数据
     */
    @Override
    public DataRecord<DataRow<DataField>> nextRecordInternal(DataRecord<DataRow<DataField>> reuse) {
        DataRecord<DataRow<DataField>> dataRecord = new DataRecord<>();
        DataRow<DataField> dataFieldDataRow = new DataRow<>(1);
        DataField<Object> dataField = new DataField<>();
        dataField.setFieldType(ColumnType.STRING);
        dataField.setValue(String.format("exampleValue:%s", currentIndex));
        dataField.setFieldKey("exampleKey");
        dataFieldDataRow.setField(0, dataField);
        dataRecord.setData(dataFieldDataRow);
        dataRecord.setTableName("ExampleTableName");
        dataRecord.setSchemaName("ExampleSchema");
        dataRecord.setTimestamp(DateUtils.getDateStr(new Date()));
        if (currentIndex++ == endIndex) {
            isComplete(true);
        }
        System.out.println("ExampleInputFormat处理数据:" + dataRecord.toString());
        return dataRecord;
    }

    /**
     * 关闭Input,释放资源
     */
    @Override
    public void closeInput() {
        logger.info("close this InputFormat");
    }

    /**
     * 快照前允许特殊处理
     *
     * @param formatState
     */
    @Override
    public void snapshot(JobFormatState formatState) {
        if (isRestore()) {
            logger.info("snapshot run! this Startup Is Restore");
        }
        logger.info("snapshot run!");
    }
}
