package org.weiwan.argus.writer.hive;

import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.weiwan.argus.core.pub.output.BaseRichOutputFormat;
import org.weiwan.argus.core.pub.pojo.JobFormatState;
import org.weiwan.argus.core.pub.config.ArgusContext;
import org.weiwan.argus.core.pub.pojo.DataRecord;
import org.weiwan.argus.core.utils.ClusterConfigLoader;

import java.io.IOException;
import java.util.List;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/20 17:03
 * @Package: org.weiwan.argus.writer.hive
 * @ClassName: HiveOutputFormat
 * @Description:
 **/
public class HiveOutputFormat extends BaseRichOutputFormat<DataRecord<Row>> {


    protected FileSystem fileSystem;

    protected HiveMetaStoreClient metaStoreClient;

    public HiveOutputFormat(ArgusContext argusContext) {
        super(argusContext);
    }


    @Override
    public void openOutput(int taskNumber, int numTasks, ArgusContext argusContext) {
        //处理文件路径
        Configuration hadoopConfig = argusContext.getHadoopConfig();
        try {
            fileSystem = FileSystem.newInstance(hadoopConfig);
            HiveConf hiveConf = ClusterConfigLoader.loadHiveConfig("hiveConf");
            metaStoreClient = new HiveMetaStoreClient(hiveConf);



            //初始化HiveMetaStore客户端连接

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
        }
    }

    @Override
    public void writerRecordInternal(DataRecord<Row> record) {
        //写出单条记录
        System.out.println(record.toString());
    }

    @Override
    public void batchWriteRecordsInternal(List<DataRecord<Row>> batchRecords) {
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
