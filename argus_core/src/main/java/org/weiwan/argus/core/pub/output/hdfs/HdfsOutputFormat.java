package org.weiwan.argus.core.pub.output.hdfs;

import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.weiwan.argus.core.pub.config.ArgusContext;
import org.weiwan.argus.core.pub.enums.CompressType;
import org.weiwan.argus.core.pub.enums.FileType;
import org.weiwan.argus.core.pub.enums.WriteMode;
import org.weiwan.argus.core.pub.output.BaseRichOutputFormat;
import org.weiwan.argus.core.pub.pojo.DataRecord;
import org.weiwan.argus.core.pub.pojo.JobFormatState;

import java.util.List;

/**
 * @Author: xiaozhennan
 * @Date: 2020/8/3 16:57
 * @Package: org.weiwan.argus.core.pub.output.hdfs
 * @ClassName: HdfsOutputFormat
 * @Description: 提供HDFS数据写出的相关能力, 可以单独的进行数据写出到HDFS
 **/
public class HdfsOutputFormat<T extends DataRecord> extends BaseRichOutputFormat<T> {


    protected String fileName;

    protected String flieSuffix;

    protected String lineDelimiter;

    protected String fieldDelimiter;

    protected boolean isCompress;

    protected CompressType compressType;

    protected WriteMode writeMode;

    protected FileType fileType;

    protected String fsDefault;

    protected FileSystem flieSystem;


    @Override
    public void configure(Configuration parameters) {
        super.configure(parameters);

    }

    public HdfsOutputFormat(ArgusContext argusContext) {
        super(argusContext);
    }

    /**
     * 打开数据源
     *
     * @param taskNumber   当前task的并行索引
     * @param numTasks     task并行度
     * @param argusContext argus上下文
     */
    @Override
    public void openOutput(int taskNumber, int numTasks, ArgusContext argusContext) {

    }

    /**
     * 写出一条记录
     *
     * @param record
     */
    @Override
    public void writerRecordInternal(T record) {

    }

    /**
     * 写出多条记录,如果不实现,会默认调用{@link BaseRichOutputFormat#writerRecordInternal(DataRecord)}
     *
     * @param batchRecords
     */
    @Override
    public void batchWriteRecordsInternal(List<T> batchRecords) {

    }

    /**
     * 关闭output,释放资源
     */
    @Override
    public void colseOutput() {

    }

    /**
     * 进行快照前处理
     *
     * @param formatState
     */
    @Override
    public void snapshot(JobFormatState formatState) {

    }
}
