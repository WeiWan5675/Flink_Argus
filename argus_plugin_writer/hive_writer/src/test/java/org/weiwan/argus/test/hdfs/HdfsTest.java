package org.weiwan.argus.test.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/30 11:34
 * @Package: org.weiwan.argus.test.hdfs
 * @ClassName: HdfsTest
 * @Description:
 **/
public class HdfsTest {

    public static void main(String[] args) throws IOException {

        Configuration configuration = new Configuration();
        String hadoopConfdir = "F:\\hadoop-common-2.6.0-bin\\etc\\hadoop";

        File dir = new File(hadoopConfdir);
        if (dir.exists() && dir.isDirectory()) {

            File[] xmlFileList = new File(hadoopConfdir).listFiles((dir1, name) -> {
                if (name.endsWith(".xml")) {
                    return true;
                }
                return false;
            });

            if (xmlFileList != null) {
                for (File xmlFile : xmlFileList) {
                    configuration.addResource(xmlFile.toURI().toURL());
                }
            }
        }


        System.out.println(configuration);
        System.out.println(configuration);
        System.out.println(configuration);
        FileSystem fileSystem = FileSystem.get(configuration);

        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/tmp/20200713131231"));


    }

}
