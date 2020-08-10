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
public class HiveOutputFormat extends BaseRichOutputFormat<DataRecord<DataRow>> {


    private static final String KEY_AUTO_CREATE_TABLE = "writer.hive.enableAutoCreateTable";
    private static final String KEY_WRITER_HIVE_USERNAME = "writer.hive.username";
    private static final String KEY_WRITER_HIVE_PASSWORD = "writer.hive.password";
    private static final String KEY_WRITER_HIVE_SCHEMA = "writer.hive.schema";
    private static final String KEY_WRITER_HIVE_TABLENAME = "writer.hive.tableName";
    private static final String KEY_WRITER_WRITERMODE = "writer.writerMode";
    private static final String KEY_WRITER_HIVE_PARTITIONFIELD = "writer.hive.partitionField";
    private static final String KEY_WRITER_HIVE_FILETYPE = "writer.hive.fileType";
    private static final String KEY_WRITER_HIVE_COMPRESSTYPE = "writer.hive.compressType";
    private static final String KEY_WRITER_HIVE_JDBCURL = "writer.hive.jdbcUrl";
    private static final String KEY_WRITER_HIVE_FIELDDELIMITER = "writer.hive.fieldDelimiter";
    private static final String KEY_WRITER_HIVE_LINEDELIMITER = "writer.hive.lineDelimiter";
    private static final String KEY_WRITER_MATCHMODE = "writer.matchMode";
    private static final String KEY_WRITER_BATCH_WRITE_MODE_ENABLE = "writer.batchWriteMode";
    private static final String KEY_WRITER_BATCH_WRITE_SIZE = "writer.batchWriteSize";
    private static final String KEY_WRITER_HIVE_DRIVE_CLASS = "writer.hive.jdbcDriveClass";


    private boolean autoCreateTable;
    private HiveTableInfo hiveTableInfo;


    protected FileSystem fileSystem;
    protected HiveMetaStoreClient metaStoreClient;
    private String jdbcUrl;
    private String userName;
    private String passWord;
    private String dbSchema;
    private String tableName;
    private String writerMode;
    private String partitionField;
    private String fileType;
    private String compressType;
    private String fieldDelimiter;
    private String lineDelimiter;
    private String matchMode;
    private boolean batchWriteMode;
    private Integer batchWriteSize;
    private String jdbcDriveClass;
    private Connection jdbcConn;

    private Table targetTable;
    private List<FieldSchema> tableFields;

    public HiveOutputFormat(ArgusContext argusContext) {
        super(argusContext);
    }


    @Override
    public void configure(org.apache.flink.configuration.Configuration parameters) {
        //属性配置
        this.jdbcUrl = writerConfig.getStringVal(KEY_WRITER_HIVE_JDBCURL);
        this.userName = writerConfig.getStringVal(KEY_WRITER_HIVE_USERNAME);
        this.passWord = writerConfig.getStringVal(KEY_WRITER_HIVE_PASSWORD);
        this.dbSchema = writerConfig.getStringVal(KEY_WRITER_HIVE_SCHEMA);
        this.tableName = writerConfig.getStringVal(KEY_WRITER_HIVE_TABLENAME);
        this.partitionField = writerConfig.getStringVal(KEY_WRITER_HIVE_PARTITIONFIELD);
        this.fileType = writerConfig.getStringVal(KEY_WRITER_HIVE_FILETYPE, "text");
        this.compressType = writerConfig.getStringVal(KEY_WRITER_HIVE_COMPRESSTYPE, "NONE");
        this.autoCreateTable = writerConfig.getBooleanVal(KEY_AUTO_CREATE_TABLE, false);
        this.fieldDelimiter = writerConfig.getStringVal(KEY_WRITER_HIVE_FIELDDELIMITER, "\u0001");
        this.lineDelimiter = writerConfig.getStringVal(KEY_WRITER_HIVE_LINEDELIMITER, "\n");

        this.matchMode = writerConfig.getStringVal(KEY_WRITER_MATCHMODE, "Alignment");
        this.writerMode = writerConfig.getStringVal(KEY_WRITER_WRITERMODE, "overwrite");
        this.batchWriteMode = writerConfig.getBooleanVal(KEY_WRITER_BATCH_WRITE_MODE_ENABLE, false);
        this.batchWriteSize = writerConfig.getIntVal(KEY_WRITER_BATCH_WRITE_SIZE, 10000);
        this.jdbcDriveClass = writerConfig.getStringVal(KEY_WRITER_HIVE_DRIVE_CLASS, JdbcFactory.JDBC_HIVE_DRIVE);
    }

    @Override
    public void openOutput(int taskNumber, int numTasks, ArgusContext argusContext) {
        //处理文件路径
        StartOptions startOptions = argusContext.getStartOptions();
        String hadoopConfDir = startOptions.getHadoopConf();
        Configuration hadoopConfig = ClusterConfigLoader.loadHadoopConfig(hadoopConfDir);
        String hiveConfDir = startOptions.getHiveConf();
        try {
            fileSystem = FileSystem.newInstance(hadoopConfig);
            HiveConf hiveConf = ClusterConfigLoader.loadHiveConfig(hiveConfDir);
            metaStoreClient = new HiveMetaStoreClient(hiveConf);
            this.hiveTableInfo = HiveTableInfo.newBuilder().jdbcUrl(jdbcUrl).driveClassName(jdbcDriveClass)
                    .dbSchema(dbSchema)
                    .tableName(tableName)
                    .passWord(passWord)
                    .userName(userName)
                    .compressType(CompressType.valueOf(compressType.toUpperCase()))
                    .enableAutoCreateTable(autoCreateTable)
                    .fieldDelimiter(fieldDelimiter)
                    .fileType(FileType.valueOf(fileType.toUpperCase()))
                    .lineDelimiter(lineDelimiter)
                    .matchMode(MatchMode.valueOf(matchMode.toUpperCase())).build();

            //HiveJdbc初始化
            Class.forName(jdbcDriveClass);
            this.jdbcConn = DriverManager.getConnection(jdbcUrl, userName, passWord);

            boolean tableReady = checkTargetTable(hiveTableInfo);

            if (autoCreateTable && !tableReady) {
                //表不存在,需要自动建表
            } else {
                //表不存在,并且没有开启自动建表,找不到表 抛出异常
                throw new RuntimeException("table not found ,please check table exist or enable auto create table");
            }

            //初始化HiveJdbc客户端连接
            //连接Hive,解析表信息
            //表信息读取校验

            //初始化文件路径,临时文件路径,文件写出名称,
            //读取Hive数据表源信息(解析数据表是什么类型的文件格式,也可以通过配置指定)
            //根据得到的源信息,创建不同的文件写入组件
            //等待写出

        } catch (IOException e) {
            e.printStackTrace();
        } catch (MetaException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private boolean checkTargetTable(HiveTableInfo waitCheck) throws TException {
        try {
            String dbSchema = waitCheck.getDbSchema();
            String tableName = waitCheck.getTableName();

            //表如果不存在,直接返回false
            if (!metaStoreClient.tableExists(dbSchema, tableName)) {
                return false;
            }

            //表存在,开始解析表信息
            this.targetTable = metaStoreClient.getTable(dbSchema, tableName);

            StorageDescriptor sd = targetTable.getSd();
            String inputFormat = sd.getInputFormat();
            String outputFormat = sd.getOutputFormat();

            SerDeInfo serdeInfo = sd.getSerdeInfo();
            Map<String, String> delimInfo = serdeInfo.getParameters();
            String lineDelim = delimInfo.get("line.delim");
            String fieldDelim = delimInfo.get("field.delim");

            if(!lineDelim.equalsIgnoreCase(waitCheck.getLineDelimiter())){
                waitCheck.setLineDelimiter(lineDelim);
            }
            if(!fieldDelim.equalsIgnoreCase(waitCheck.getFieldDelimiter())){
                waitCheck.setFieldDelimiter(fieldDelim);
            }




            this.tableFields = sd.getCols();

            waitCheck.setInputClass(inputFormat);
            waitCheck.setOutputClass(outputFormat);


            //表序列化信息准确
            //表分隔符信息准确
            //表压缩信息准确
            //表数据路径准确
            //修正各种表数据
//            org.apache.hadoop.mapred.TextInputFormat
//            org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
//            org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
            //Table准备好
        } catch (TException e) {
            e.printStackTrace();
        }
        return true;
    }

    @Override
    public void writerRecordInternal(DataRecord<DataRow> record) {
        //写出单条记录
        System.out.println(record.toString());
    }

    @Override
    public void batchWriteRecordsInternal(List<DataRecord<DataRow>> batchRecords) {
        //批量写出记录
    }

    @Override
    public void colseOutput() {
        //关闭Output
    }

    @Override
    public void snapshot(JobFormatState formatState) {
        //快照操作
    }
}
