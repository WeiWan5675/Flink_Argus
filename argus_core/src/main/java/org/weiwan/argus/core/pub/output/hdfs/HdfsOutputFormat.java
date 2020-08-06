package org.weiwan.argus.core.pub.output.hdfs;

import org.apache.commons.lang3.CharEncoding;
import org.apache.commons.lang3.CharSet;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.ql.exec.TextRecordWriter;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.weiwan.argus.core.pub.config.ArgusContext;
import org.weiwan.argus.core.pub.config.WriterConfig;
import org.weiwan.argus.core.pub.enums.CompressType;
import org.weiwan.argus.core.pub.enums.FileType;
import org.weiwan.argus.core.pub.enums.WriteMode;
import org.weiwan.argus.core.pub.output.BaseRichOutputFormat;
import org.weiwan.argus.core.pub.pojo.DataRecord;
import org.weiwan.argus.core.pub.pojo.JobFormatState;
import org.weiwan.argus.core.start.StartOptions;
import org.weiwan.argus.core.utils.ClusterConfigLoader;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.List;

/**
 * @Author: xiaozhennan
 * @Date: 2020/8/3 16:57
 * @Package: org.weiwan.argus.core.pub.output.hdfs
 * @ClassName: HdfsOutputFormat
 * @Description: 提供HDFS数据写出的相关能力, 数据写出到HDFS
 **/
public class HdfsOutputFormat<T extends DataRecord> extends BaseRichOutputFormat<T> {

    protected String fileName;
    protected String targetPath;
    protected String fileSuffix;
    protected String lineDelimiter = "\n";
    protected String fieldDelimiter = "\u0001";

    protected CompressType compressType = CompressType.NONE;
    protected WriteMode writeMode = WriteMode.OVERWRITE;
    protected FileType fileType = FileType.TEXT;

    protected String tmpPath;
    protected String tmpFileName;
    protected String tmpFileSuffix = "tmp";

    protected String dfsDefault;
    private String charsetName;

    protected FileSystem flieSystem;
    private RecordWriter recordWriter;
    private AbstractSerDe serDe;
    private StructObjectInspector inspector;
    private FileOutputFormat outputFormat;
    private JobConf jobConf;

    public static final String WRITER_HDFS_OUTPUT_FILE_NAME = "writer.hdfs.output.fileName";
    public static final String WRITER_HDFS_OUTPUT_FILE_SUFFIX = "writer.hdfs.output.fileSuffix";
    public static final String WRITER_HDFS_OUTPUT_PATH = "writer.hdfs.output.dir";
    public static final String WRITER_HDFS_OUTPUT_LINEDELIMITER = "writer.hdfs.output.lineDelimiter";
    public static final String WRITER_HDFS_OUTPUT_FIELDDELIMITER = "writer.hdfs.output.fieldDelimiter";
    public static final String WRITER_HDFS_OUTPUT_CHARSETNAME = "writer.hdfs.output.charSetName";

    public static final String WRITER_HDFS_DFSDEFAULT = "writer.hdfs.dfsDefault";
    private String currentFileBlock;
    private org.apache.hadoop.conf.Configuration configuration;
    @Override
    public void configure(Configuration parameters) {
        //初始化配置文件
        this.fileName = writerConfig.getStringVal(WRITER_HDFS_OUTPUT_FILE_NAME, new Date().getTime() + "");
        this.targetPath = writerConfig.getStringVal(WRITER_HDFS_OUTPUT_PATH);
        this.fileSuffix = writerConfig.getStringVal(WRITER_HDFS_OUTPUT_FILE_SUFFIX);
        this.lineDelimiter = writerConfig.getStringVal(WRITER_HDFS_OUTPUT_LINEDELIMITER, "\n");
        this.fieldDelimiter = writerConfig.getStringVal(WRITER_HDFS_OUTPUT_FIELDDELIMITER, "\u0001");
        this.charsetName = writerConfig.getStringVal(WRITER_HDFS_OUTPUT_CHARSETNAME, CharEncoding.UTF_8);
        this.dfsDefault = writerConfig.getStringVal(WRITER_HDFS_DFSDEFAULT);
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
        StartOptions startOptions = argusContext.getStartOptions();
        String hadoopConfDir = startOptions.getHadoopConf();
        this.configuration = ClusterConfigLoader.loadHadoopConfig(hadoopConfDir);


        //初始化成员变量
        checkFormatVars();
        //初始化文件夹/临时文件夹/临时文件/目标文件名称
        if (StringUtils.isBlank(fileName)) {
            //为空,根据任务index生成文件名称
            this.fileName = "0000" + taskNumber + fileSuffix;
        }
        //临时目录
        this.tmpPath = targetPath + ".temporary";
        this.tmpFileName = fileName + tmpFileSuffix;
        if (isRestore()) {
            this.tmpFileName = "." + fileName + tmpFileSuffix;
        }
        this.currentFileBlock = tmpPath + File.separator + tmpFileName;

//        FileOutputFormat fileOutputFormat = FileOutputFacory.generateOutputFormat(fileType, compressType, configuration);
        ParquetOutputFormat<Object> opof = new ParquetOutputFormat<>();


        /**
         * HDFS :
         * HIVE :
         *
         */

        try {
            flieSystem = FileSystem.get(configuration);

            //初始化目录
            /**
             * 目标目录
             * 临时目录
             * 如果目标目录有数据,判断文件写入模式
             */

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void checkFormatVars() {
        if (StringUtils.isBlank(targetPath)) {
            throw new RuntimeException("target path cant is null,please check the output dir");
        }
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
