package org.weiwan.argus.core.pub.output;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.weiwan.argus.core.pub.pojo.JobFormatState;
import org.weiwan.argus.core.pub.config.ArgusContext;
import org.weiwan.argus.core.pub.config.JobConfig;
import org.weiwan.argus.core.pub.config.WriterConfig;
import org.weiwan.argus.core.pub.pojo.DataRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/14 17:23
 * @Package: org.weiwan.argus.pub.api
 * @ClassName: BaseRichOutputFormat
 * @Description: BaseRichOutputFormat 负责处理数据,维护状态,调用子类处理数据的方法, 提供Sink调用的方法
 **/
public abstract class BaseRichOutputFormat<T extends DataRecord> extends RichOutputFormat<T> {

    protected ArgusContext argusContext;
    protected JobConfig jobConfig;
    protected WriterConfig writerConfig;
    protected JobFormatState formatState;

    protected int taskNumber;
    protected int numTasks;

    protected List<T> batchRecords;
    protected boolean isBatchWriteMode;
    protected int batchWriteSize;

    private boolean isRestore;


    /**
     * 打开数据源
     *
     * @param taskNumber   当前task的并行索引
     * @param numTasks     task并行度
     * @param argusContext argus上下文
     */
    public abstract void openOutput(int taskNumber, int numTasks, ArgusContext argusContext);


    /**
     * 写出一条记录
     *
     * @param record
     */
    public abstract void writerRecordInternal(T record);


    /**
     * 写出多条记录,如果不实现,会默认调用{@link BaseRichOutputFormat#writerRecordInternal(DataRecord)}
     *
     * @param batchRecords
     */
    public abstract void batchWriteRecordsInternal(List<T> batchRecords);


    /**
     * 关闭output,释放资源
     */
    public abstract void colseOutput() throws IOException;

    /**
     * 进行快照前处理
     *
     * @param formatState
     */
    public abstract void snapshot(JobFormatState formatState);


    public BaseRichOutputFormat(ArgusContext argusContext) {
        this.argusContext = argusContext;
        this.jobConfig = argusContext.getJobConfig();
        this.writerConfig = argusContext.getJobConfig().getWriterConfig();
    }


    @Override
    public void configure(Configuration parameters) {
        //什么都不做,如果子类重写也方便
        System.out.println("output format configure");
    }


    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        this.isBatchWriteMode = writerConfig.getBooleanVal("writer.batchWriteMode", false);
        this.batchWriteSize = writerConfig.getIntVal("writer.batchWriteSize", 1000);

        if (isBatchWriteMode) {
            this.batchRecords = new ArrayList(batchWriteSize);
        }
        this.taskNumber = taskNumber;
        this.numTasks = numTasks;
        if (!isRestore()) {
            //不是Restore 需要手动创建formatstate
            formatState = new JobFormatState();
        }
        //子类打开资源
        openOutput(taskNumber, numTasks, argusContext);
        //子类初始化完成
    }


    @Override
    public void writeRecord(T record) throws IOException {
        if (isBatchWriteMode && batchWriteSize > 1) {
            //批处理模式
            batchRecords.add(record);
            if (batchRecords.size() == batchWriteSize)
                writeRecords();
        } else {
            //逐条处理模式
            writerRecordInternal(record);
        }
    }

    private void writeRecords() {
        try {
            batchWriteRecordsInternal(batchRecords);
        } catch (Exception e) {
            //写入异常
            e.printStackTrace();
            //变成逐条处理
            batchRecords.forEach(this::writerRecordInternal);
        }
        batchRecords.clear();
    }


    @Override
    public void close() throws IOException {
        if (isBatchWriteMode && batchWriteSize > 0) {
            //关闭前将批处理的都写出去
            writeRecords();
        }
        colseOutput();
    }

    public JobFormatState getSnapshotState() {
        snapshot(formatState);
        return this.formatState;
    }


    public boolean isRestore(boolean... flags) {
        if (flags.length == 1) {
            this.isRestore = flags[0];
        }
        return this.isRestore;
    }

    public void setJobFormatState(JobFormatState jobFormatState) {
        this.formatState = jobFormatState;
    }
}
