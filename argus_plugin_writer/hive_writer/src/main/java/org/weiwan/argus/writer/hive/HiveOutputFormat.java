package org.weiwan.argus.writer.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.thrift.TException;
import org.weiwan.argus.core.pub.enums.CompressType;
import org.weiwan.argus.core.pub.enums.FileType;
import org.weiwan.argus.core.pub.output.BaseRichOutputFormat;
import org.weiwan.argus.core.pub.output.hdfs.HdfsOutputFormat;
import org.weiwan.argus.core.pub.output.hdfs.MatchMode;
import org.weiwan.argus.core.pub.pojo.DataRow;
import org.weiwan.argus.core.pub.pojo.JobFormatState;
import org.weiwan.argus.core.pub.config.ArgusContext;
import org.weiwan.argus.core.pub.pojo.DataRecord;
import org.weiwan.argus.core.start.StartOptions;
import org.weiwan.argus.core.utils.ClusterConfigLoader;
import org.weiwan.argus.core.utils.JdbcFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.Map;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/20 17:03
 * @Package: org.weiwan.argus.writer.hive
 * @ClassName: HiveOutputFormat
 * @Description:
 **/
public class HiveOutputFormat extends HdfsOutputFormat<DataRecord<DataRow>> {


    private static final String KEY_WRITER_HIVE_JDBCURL = "writer.hive.jdbcUrl";
    private static final String KEY_WRITER_HIVE_FIELDDELIMITER = "writer.hive.fieldDelimiter";
    private static final String KEY_WRITER_HIVE_LINEDELIMITER = "writer.hive.lineDelimiter";
    private static final String KEY_WRITER_HIVE_PARTITIONFIELD = "writer.hive.partitionField";
    private static final String KEY_WRITER_HIVE_DRIVE_CLASS = "writer.hive.jdbcDriveClass";
    private static final String KEY_WRITER_HIVE_USERNAME = "writer.hive.username";
    private static final String KEY_WRITER_HIVE_PASSWORD = "writer.hive.password";
    private static final String KEY_WRITER_HIVE_SCHEMA = "writer.hive.schema";
    private static final String KEY_WRITER_HIVE_TABLENAME = "writer.hive.tableName";
    private static final String KEY_WRITER_HIVE_AUTO_CREATE_TABLE = "writer.hive.enableAutoCreateTable";


    private static final String KEY_WRITER_WRITERMODE = "writer.writerMode";
    private static final String KEY_WRITER_HIVE_FILETYPE = "writer.hive.fileType";
    private static final String KEY_WRITER_HIVE_COMPRESSTYPE = "writer.hive.compressType";
    private static final String KEY_WRITER_BATCH_WRITE_MODE_ENABLE = "writer.batchWriteMode";
    private static final String KEY_WRITER_BATCH_WRITE_SIZE = "writer.batchWriteSize";
    private static final String KEY_WRITER_MATCHMODE = "writer.matchMode";


    private String jdbcDriveClass;
    private String jdbcUrl;
    private String userName;
    private String passWord;
    private String dbSchema;
    private String tableName;
    private String partitionField;
    private boolean autoCreateTable;


    private HiveTableInfo hiveTableInfo;
    protected HiveMetaStoreClient metaStoreClient;
    private Connection jdbcConn;

    private Table targetTable;
    private List<FieldSchema> tableFields;


    public HiveOutputFormat(ArgusContext argusContext) {
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
        //初始化相关环境变量
        this.jdbcUrl = writerConfig.getStringVal(KEY_WRITER_HIVE_JDBCURL);
        this.jdbcDriveClass = writerConfig.getStringVal(KEY_WRITER_HIVE_DRIVE_CLASS);
        this.tableName = writerConfig.getStringVal(KEY_WRITER_HIVE_TABLENAME);
        this.userName = writerConfig.getStringVal(KEY_WRITER_HIVE_USERNAME);
        this.passWord = writerConfig.getStringVal(KEY_WRITER_HIVE_PASSWORD);
        this.partitionField = writerConfig.getStringVal(KEY_WRITER_HIVE_PARTITIONFIELD);
        this.dbSchema = writerConfig.getStringVal(KEY_WRITER_HIVE_SCHEMA, "default");
        this.autoCreateTable = writerConfig.getBooleanVal(KEY_WRITER_HIVE_AUTO_CREATE_TABLE, false);


        //从Hive表获取元信息

        //使用元信息初始化HDFS_OUTPUT


        super.openOutput(taskNumber, numTasks, argusContext);
    }

    /**
     * 关闭output,释放资源
     */
    @Override
    public void colseOutput() throws IOException {
        super.colseOutput();
    }
}
