package org.weiwan.argus.core.pub.output.hdfs;

import org.apache.commons.lang3.CharEncoding;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.weiwan.argus.core.pub.config.ArgusContext;
import org.weiwan.argus.core.pub.enums.CompressType;
import org.weiwan.argus.core.pub.enums.FileType;
import org.weiwan.argus.core.pub.enums.WriteMode;
import org.weiwan.argus.core.pub.output.BaseRichOutputFormat;
import org.weiwan.argus.core.pub.pojo.DataField;
import org.weiwan.argus.core.pub.pojo.DataRecord;
import org.weiwan.argus.core.pub.pojo.DataRow;
import org.weiwan.argus.core.pub.pojo.JobFormatState;
import org.weiwan.argus.core.utils.HadoopUtil;

import java.io.IOException;
import java.util.Date;
import java.util.List;

/**
 * @Author: xiaozhennan
 * @Date: 2020/9/3 17:06
 * @Package: org.weiwan.argus.core.pub.output.hdfs.HdfsOutputFormatV2
 * @ClassName: HdfsOutputFormatV2
 * @Description:
 **/
@SuppressWarnings("all")
public class HdfsOutputFormatV2 extends BaseRichOutputFormat<DataRecord<DataRow<DataField>>> {
    public HdfsOutputFormatV2(ArgusContext argusContext) {
        super(argusContext);
    }

    private static final Logger logger = LoggerFactory.getLogger(HdfsOutputFormat.class);

    public static final String WRITER_HDFS_OUTPUT_FILE_NAME = "writer.output.fileName";
    public static final String WRITER_HDFS_OUTPUT_FILE_SUFFIX = "writer.output.fileSuffix";
    public static final String WRITER_HDFS_OUTPUT_PATH = "writer.output.dir";
    public static final String WRITER_HDFS_OUTPUT_LINEDELIMITER = "writer.output.lineDelimiter";
    public static final String WRITER_HDFS_OUTPUT_FIELDDELIMITER = "writer.output.fieldDelimiter";
    public static final String WRITER_HDFS_OUTPUT_CHARSETNAME = "writer.output.charSetName";
    public static final String WRITER_HDFS_OUTPUT_MATCHMODE = "writer.output.matchMode";
    public static final String WRITER_HDFS_OUTPUT_WRITERMODE = "writer.output.writeMode";
    public static final String WRITER_HDFS_OUTPUT_COMPRESSTYPE = "writer.output.compressType";
    public static final String WRITER_HDFS_OUTPUT_FILETYPE = "writer.output.fileType";
    private static final String WRITER_HDFS_OUTPUT_COMBINESMALLFILES = "writer.output.combineSmallFiles";
    public static final String WRITER_HDFS_OUTPUT_FILE_BLOCKSIZE = "writer.output.fileBlockSize";
    public static final String WRITER_HDFS_DFSDEFAULT = "writer.dfsDefault";

    protected String fileName;
    protected String targetPath;
    protected String fileSuffix;
    protected String lineDelimiter = "\n";
    protected String fieldDelimiter = "\u0001";
    protected CompressType compressType = CompressType.NONE;
    protected WriteMode writeMode = WriteMode.OVERWRITE;
    protected FileType fileType = FileType.TEXT;
    protected MatchMode matchMode = MatchMode.ALIGNMENT;
    protected boolean combineSmallFiles;
    protected String tmpPath;
    protected String tmpFileName;
    protected String actionPath;
    protected String tmpFileSuffix = "tmp";
    protected String dfsDefault;
    protected String charsetName;

    protected FileOutputer outPuter;
    protected List<DataField> dataFields;
    protected FileSystem fileSystem;
    protected org.apache.hadoop.conf.Configuration hadoopConfig;

    @Override
    public void configure(Configuration parameters) {
        super.configure(parameters);
        //初始化配置文件
        this.fileName = writerConfig.getStringVal(WRITER_HDFS_OUTPUT_FILE_NAME, "part_0000");
        this.targetPath = writerConfig.getStringVal(WRITER_HDFS_OUTPUT_PATH);
        this.fileSuffix = writerConfig.getStringVal(WRITER_HDFS_OUTPUT_FILE_SUFFIX, ".dat");
        this.lineDelimiter = writerConfig.getStringVal(WRITER_HDFS_OUTPUT_LINEDELIMITER, "\n");
        this.fieldDelimiter = writerConfig.getStringVal(WRITER_HDFS_OUTPUT_FIELDDELIMITER, "\u0001");
        this.charsetName = writerConfig.getStringVal(WRITER_HDFS_OUTPUT_CHARSETNAME, CharEncoding.UTF_8);
        this.dfsDefault = writerConfig.getStringVal(WRITER_HDFS_DFSDEFAULT);
        this.matchMode = MatchMode.valueOf(writerConfig.getStringVal(WRITER_HDFS_OUTPUT_MATCHMODE, "ALIGNMENT").toUpperCase());
        this.writeMode = WriteMode.valueOf(writerConfig.getStringVal(WRITER_HDFS_OUTPUT_WRITERMODE, "APPEND").toUpperCase());
        this.compressType = CompressType.valueOf(writerConfig.getStringVal(WRITER_HDFS_OUTPUT_COMPRESSTYPE, "NONE").toUpperCase());
        this.fileType = FileType.valueOf(writerConfig.getStringVal(WRITER_HDFS_OUTPUT_FILETYPE, "TEXT").toUpperCase());
        this.combineSmallFiles = writerConfig.getBooleanVal(WRITER_HDFS_OUTPUT_COMBINESMALLFILES, false);
    }


    @Override
    public void openOutput(int taskNumber, int numTasks, ArgusContext argusContext) {
        //打开文件系统

        try {
            this.hadoopConfig = HadoopUtil.getConfiguration(jobConfig);
            this.fileSystem = HadoopUtil.getFileSystem(hadoopConfig);


            //初始化文件写出器
            //生成文件目录,临时目录
            //打开文件系统
            if (isRestore()) {
                //需要进行恢复工作
                /**
                 * 1. 把临时目录中,checkpointIndex小于state中的index的全都移动到完成目录,然后清空工作目录
                 */
            }


        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void writerRecordInternal(DataRecord<DataRow<DataField>> record) {

    }


    @Override
    public void batchWriteRecordsInternal(List<DataRecord<DataRow<DataField>>> batchRecords) {

    }


    @Override
    public void closeOutput() throws IOException {

    }


    @Override
    public void snapshot(JobFormatState formatState) {
        //把当前正在写入的文件刷新,关闭文件
        //把
    }

    @Override
    public void notifyCheckpointComplete(long currentCheckpointIndex, long nextCheckpointIndex) {
        //把当前checkpointCompleteID下的所有文件移动到完成目录
        //日过开启了合并小文件,需要移动时合并小文件
    }
}
