package org.weiwan.argus.writer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.weiwan.argus.core.pub.config.ArgusContext;
import org.weiwan.argus.core.pub.output.BaseRichOutputFormat;
import org.weiwan.argus.core.pub.pojo.DataField;
import org.weiwan.argus.core.pub.pojo.DataRecord;
import org.weiwan.argus.core.pub.pojo.DataRow;
import org.weiwan.argus.core.pub.pojo.JobFormatState;

import java.io.IOException;
import java.util.List;

/**
 * @Author: xiaozhennan
 * @Date: 2020/8/12 16:27
 * @Package: org.weiwan.argus.writer.ExampleOutputFormat
 * @ClassName: ExampleOutputFormat
 * @Description:
 **/
public class ExampleOutputFormat extends BaseRichOutputFormat<DataRecord<DataRow<DataField>>> {


    public ExampleOutputFormat(ArgusContext argusContext) {
        super(argusContext);
    }
    public static final Logger logger = LoggerFactory.getLogger(ExampleOutputFormat.class);

    private String exampleVar;

    /**
     * 打开数据源
     *
     * @param taskNumber   当前task的并行索引
     * @param numTasks     task并行度
     * @param argusContext argus上下文
     */
    @Override
    public void openOutput(int taskNumber, int numTasks, ArgusContext argusContext) {
        exampleVar = writerConfig.getStringVal("writer.example.writerVar");
        logger.info("example output format open");
    }

    /**
     * 写出一条记录
     *
     * @param record
     */
    @Override
    public void writerRecordInternal(DataRecord<DataRow<DataField>> record) {
        System.out.println("ExampleOutputFormat处理数据:" + record.toString());
    }

    /**
     * 写出多条记录,如果不实现,会默认调用{@link BaseRichOutputFormat#writerRecordInternal(DataRecord)}
     *
     * @param batchRecords
     */
    @Override
    public void batchWriteRecordsInternal(List<DataRecord<DataRow<DataField>>> batchRecords) {

    }

    /**
     * 关闭output,释放资源
     */
    @Override
    public void closeOutput() throws IOException {
        logger.info("close example output format");
    }

    /**
     * 进行快照前处理
     *
     * @param formatState
     */
    @Override
    public void snapshot(JobFormatState formatState) {
        logger.info("snapshot run!");
    }
}
