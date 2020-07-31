package org.weiwan.argus.test.hdfs;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.thrift.TException;
import org.weiwan.argus.core.utils.ClusterConfigLoader;

import java.util.List;
import java.util.Map;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/31 11:10
 * @Package: org.weiwan.argus.test.hdfs
 * @ClassName: HiveTest
 * @Description:
 **/
public class HiveTest {

    public static void main(String[] args) throws Exception {
        String confDir = "F:\\hadoop-common-2.6.0-bin\\etc\\hadoop";
        HiveConf hiveConf = ClusterConfigLoader.loadHiveConfig(confDir);

        HiveMetaStoreClient client = new HiveMetaStoreClient(hiveConf);

        List<String> allDatabases = client.getAllDatabases();

        for (String allDatabase : allDatabases) {
            System.out.println(allDatabase);
        }

        List<String> easylife_ods1 = client.getAllTables("easylife_ods");

        for (String s : easylife_ods1) {
            System.out.println(s);
        }

        Database easylife_ods = client.getDatabase("default");

        System.out.println(easylife_ods);

        Table table = client.getTable("easylife_ods", "ods_easylife_order");
        StorageDescriptor sd = table.getSd();
        String inputFormat = sd.getInputFormat();

        String outputFormat = sd.getOutputFormat();
        SerDeInfo serdeInfo = sd.getSerdeInfo();
        boolean compressed = sd.isCompressed();



        System.out.println(table);

        Map<String, String> parameters = table.getParameters();

        System.out.println(parameters);


    }
}
