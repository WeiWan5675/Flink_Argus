package org.weiwan.argus.core.pub.output.hdfs;

import org.apache.commons.lang3.CharEncoding;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.weiwan.argus.common.utils.SystemUtil;
import org.weiwan.argus.core.pub.config.ArgusContext;
import org.weiwan.argus.core.pub.enums.CompressType;
import org.weiwan.argus.core.pub.enums.FileType;
import org.weiwan.argus.core.pub.enums.WriteMode;
import org.weiwan.argus.core.pub.output.BaseRichOutputFormat;
import org.weiwan.argus.core.pub.pojo.DataField;
import org.weiwan.argus.core.pub.pojo.DataRecord;
import org.weiwan.argus.core.pub.pojo.JobFormatState;
import org.weiwan.argus.core.start.StartOptions;
import org.weiwan.argus.core.utils.ClusterConfigLoader;
import org.weiwan.argus.core.utils.HadoopUtil;
import org.weiwan.argus.core.utils.HdfsUtil;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
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

    private static final Logger logger = LoggerFactory.getLogger(HdfsOutputFormat.class);

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
    protected String actionPath;
    protected String tmpFileSuffix = "tmp";

    protected String dfsDefault;
    private String charsetName;

    protected FileOutputer outPuter;
    protected List<DataField> dataFields;
    protected FileSystem fileSystem;


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
    public static final String WRITER_HDFS_OUTPUT_FILE_BLOCKSIZE = "writer.output.fileBlockSize";
    public static final String WRITER_HDFS_DFSDEFAULT = "writer.dfsDefault";

    private String currentFileBlock;

    private int currentFileBlockIndex = 0;
    private int nextFileBlockIndex = 0;
    private org.apache.hadoop.conf.Configuration configuration;
    private MatchMode matchMode;
    private List<String> completeFileBlocks = new ArrayList<>();
    private List<String> waitFinishdFileBlocks = new ArrayList<>();
    private List<String> actionFileBlocks = new ArrayList<>();


    @Override
    public void configure(Configuration parameters) {
        //初始化配置文件
        this.fileName = writerConfig.getStringVal(WRITER_HDFS_OUTPUT_FILE_NAME, new Date().getTime() + "");
        this.targetPath = writerConfig.getStringVal(WRITER_HDFS_OUTPUT_PATH);
        this.fileSuffix = writerConfig.getStringVal(WRITER_HDFS_OUTPUT_FILE_SUFFIX, "");
        this.lineDelimiter = writerConfig.getStringVal(WRITER_HDFS_OUTPUT_LINEDELIMITER, "\n");
        this.fieldDelimiter = writerConfig.getStringVal(WRITER_HDFS_OUTPUT_FIELDDELIMITER, "\u0001");
        this.charsetName = writerConfig.getStringVal(WRITER_HDFS_OUTPUT_CHARSETNAME, CharEncoding.UTF_8);
        this.dfsDefault = writerConfig.getStringVal(WRITER_HDFS_DFSDEFAULT);
        this.matchMode = MatchMode.valueOf(writerConfig.getStringVal(WRITER_HDFS_OUTPUT_MATCHMODE, "ALIGNMENT").toUpperCase());
        this.writeMode = WriteMode.valueOf(writerConfig.getStringVal(WRITER_HDFS_OUTPUT_WRITERMODE, "APPEND").toUpperCase());
        this.compressType = CompressType.valueOf(writerConfig.getStringVal(WRITER_HDFS_OUTPUT_COMPRESSTYPE, "NONE").toUpperCase());
        this.fileType = FileType.valueOf(writerConfig.getStringVal(WRITER_HDFS_OUTPUT_FILETYPE, "TEXT").toUpperCase());


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
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        this.configuration = ClusterConfigLoader.loadHadoopConfig(startOptions);
        configuration.set("dfs.socket.timeout", "6000000");

        try {
            fileSystem = FileSystem.get(configuration);
            //初始化成员变量
//        checkFormatVars();
            if (WriteMode.OVERWRITE == writeMode) {
                //是覆盖写入,直接删除文件夹
                HdfsUtil.deleteFile(new Path(targetPath), fileSystem, true);
            }
            //初始化文件夹/临时文件夹/临时文件/目标文件名称
            String p = nextFileBlock(taskNumber);
            initOutputer();

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private String nextFileBlock(int taskNumber) {
        this.currentFileBlockIndex = nextFileBlockIndex;
        if (StringUtils.isBlank(fileName)) {
            //为空,根据任务index生成文件名称
            this.fileName = "part_0000" + currentFileBlockIndex + "_" + taskNumber + fileSuffix;
        } else {
            this.fileName = fileName + "_" + currentFileBlockIndex + "_" + taskNumber + fileSuffix;
        }
        //临时目录
        this.tmpPath = targetPath + File.separator + ".temporary";
        this.actionPath = targetPath + File.separator + ".action";
        this.tmpFileName = "." + fileName + "." + tmpFileSuffix;
        if (isRestore()) {
            this.tmpFileName = "." + fileName + "." + tmpFileSuffix;
        }
        this.currentFileBlock = tmpPath + File.separator + tmpFileName;
        this.nextFileBlockIndex = currentFileBlockIndex + 1;
        //添加一个当前完成的completeFile path
        completeFileBlocks.add(targetPath + File.separator + fileName);
        //添加等待完成的,正在写入的文件路径
        waitFinishdFileBlocks.add(currentFileBlock);
        //添加一个完成后移动到指定目录的文件路径
        actionFileBlocks.add(actionPath + File.separator + fileName);
        return currentFileBlock;
    }

    private void initOutputer() {
        switch (fileType) {
            case TEXT:
                outPuter = new TextFileOutputer(configuration, fileSystem);
                break;
            case ORC:
                outPuter = new OrcFileOutputer(configuration, fileSystem);
                break;
            case PARQUET:
                outPuter = new ParquetFileOutputer(configuration, fileSystem);
                break;
            default:
                System.out.println("未匹配");
        }

        try {
            BaseFileOutputer outputer = ((BaseFileOutputer) outPuter);
            outputer.setCharsetName(charsetName);
            outputer.setCompressType(compressType);
            outputer.setFileType(fileType);
            outputer.setMatchMode(matchMode);
            outputer.setBlockPath(currentFileBlock);
            outputer.setFieldDelimiter(fieldDelimiter);
            outputer.setLineDelimiter(lineDelimiter);
            outputer.setBatchWriteMode(isBatchWriteMode);
            outputer.setBatchWriteSize(batchWriteSize);
            outPuter.init(dataFields);
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
        try {
            outPuter.output(record);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 写出多条记录,如果不实现,会默认调用{@link BaseRichOutputFormat#writerRecordInternal(DataRecord)}
     *
     * @param batchRecords
     */
    @Override
    public void batchWriteRecordsInternal(List<T> batchRecords) {
        for (T batchRecord : batchRecords) {
            try {
                outPuter.output(batchRecord);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 关闭output,释放资源
     */
    @Override
    public void closeOutput() throws IOException {
        //文件写入器先释放
        outPuter.close();
        //删除临时文件
        try {
            //移动文件到actionDir
            moveDataToActionDir();
            //等待所有任务完成
            waitAllTaskComplete();
            //移动文件到目标文件夹
            moveDataToTargetDir();
            //等待所有节点移动完成
            waitMoveDataComplete();
            //清理工作空间
            cleanUpTheWorkspace();
        } catch (IOException e) {
            e.printStackTrace();
            throw e;
        } finally {
            try {
                //不管任务是否正常完成,删除临时目录
                HdfsUtil.deleteFile(new Path(tmpPath), fileSystem, true);
                Path _action = new Path(actionPath);
                if (fileSystem.exists(_action) && fileSystem.listStatus(_action).length == 0) {
                    //可以删除了
                    HdfsUtil.deleteFile(new Path(actionPath), fileSystem, true);
                }
            } catch (IllegalArgumentException e) {
                e.printStackTrace();
            }
        }
    }

    private void cleanUpTheWorkspace() {
        logger.info("start clean up the work space");
        Path actionDir = new Path(actionPath);
        Path tmpDir = new Path(tmpPath);

        try {
            if (fileSystem.exists(actionDir)) {
                logger.info("delete action dir:{}", actionDir.toString());
                HdfsUtil.deleteFile(actionDir, fileSystem, true);
            }

            if (fileSystem.exists(tmpDir)) {
                logger.info("delete tmp dir:{}", tmpDir.toString());
                HdfsUtil.deleteFile(tmpDir, fileSystem, true);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void waitMoveDataComplete() throws IOException {
        logger.info("wait move data to target dir");
        Path actionDir = new Path(actionPath);
        while (true) {
            try {
                if (fileSystem.exists(actionDir)) {
                    FileStatus[] fileStatuses = fileSystem.listStatus(actionDir);
                    if (fileStatuses.length == 0) {
                        //都已经移动完成,跳出等待
                        logger.info("move operating finished");
                        break;
                    } else {
                        //还未完成,等待一会
                        logger.info("move operating not finished yet wait {} second", 3);
                        SystemUtil.sleep(3000);
                        continue;
                    }
                } else {
                    //action文件夹已经不存在,跳出等待
                    logger.info("action folder has been deleted by other nodes");
                    break;
                }
            } catch (IOException e) {
                logger.warn("Wait for the move to complete and delete the folder to be completed by other nodes for subsequent processing");
                break;
            }
        }
    }

    private void moveDataToActionDir() throws IOException {
        logger.info("start moving data to .action path");
        if (!fileSystem.exists(new Path(actionPath))) {
            try {
                fileSystem.mkdirs(new Path(actionPath));
                logger.info(".action does not exist ,create .action");
            } catch (IOException e) {
                e.printStackTrace();
            } catch (IllegalArgumentException e) {
                e.printStackTrace();
            }
        }
        //临时文件修改为实际complete文件
        if (waitFinishdFileBlocks.size() != 0 && completeFileBlocks.size() == waitFinishdFileBlocks.size()) {
            for (int i = 0; i < waitFinishdFileBlocks.size(); i++) {
                Path src = new Path(waitFinishdFileBlocks.get(i));
                Path actionDsc = new Path(actionFileBlocks.get(i));
                logger.info("移动文件 scr:{} to dsc: {}", src.toString(), actionDsc.toString());
                HdfsUtil.moveBlockToTarget(src, actionDsc, fileSystem, true);
            }
        }
    }

    private void moveDataToTargetDir() throws IOException {
        logger.info("move data to target dir, target dir is {}", targetPath);
        for (int i = 0; i < actionFileBlocks.size(); i++) {
            Path src = new Path(actionFileBlocks.get(i));
            Path dsc = new Path(completeFileBlocks.get(i));
            if (fileSystem.exists(src)) {
                //存在,需要移动
                logger.info("move block src:{} To dsc:{}", src.toString(), dsc.toString());
                HdfsUtil.moveBlockToTarget(src, dsc, fileSystem, true);
            }
        }
    }

    private void waitAllTaskComplete() throws IOException {
        Path actionDir = new Path(actionPath);
        logger.info("wait all task complete");
        //写文件等待次数
        int maxWaitRoundNum = 100;
        //写文件等待时间间隔
        long roundWaitTime = 3000L;

        long maxWaitTime = maxWaitRoundNum * roundWaitTime;
        while (maxWaitRoundNum != 0) {
            if (fileSystem.exists(actionDir)) {
                FileStatus[] fileStatuses = fileSystem.listStatus(actionDir);
                if (fileStatuses.length == numTasks) {
                    //所有的都到位了
                    logger.info("ALL task complete! actionFileSize:{}", fileStatuses.length);
                    break;
                } else {
                    logger.info("wait task completed, waiting {} second!", maxWaitTime - (maxWaitRoundNum * roundWaitTime));
                    SystemUtil.sleep(roundWaitTime);
                }
            }
            maxWaitRoundNum--;
        }

        if (maxWaitRoundNum == 0) {
            throw new RuntimeException("timeout for write block");
        }

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
