package org.weiwan.argus.core.utils;

import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/31 10:09
 * @Package: org.weiwan.argus.core.utils
 * @ClassName: HadoopConfigLoader
 * @Description:
 **/
public class ClusterConfigLoader {


    public static void main(String[] args) {
        Configuration configuration = ClusterConfigLoader.loadHadoopConfig("F:\\hadoop-common-2.6.0-bin\\etc\\hadoop");
        System.out.println(configuration);
    }


    public static Configuration loadHadoopConfig(String confDir) {
        Configuration configuration = new Configuration();
        try {
            File dir = new File(confDir);
            if (dir.exists() && dir.isDirectory()) {

                File[] xmlFileList = new File(confDir).listFiles((dir1, name) -> {
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
        } catch (IOException e) {
            e.printStackTrace();
        }
        return configuration;
    }


    public static YarnConfiguration loadYarnConfig(String yarnConfDir) {
        YarnConfiguration yarnConf = new YarnConfiguration();
        try {

            File dir = new File(yarnConfDir);
            if (dir.exists() && dir.isDirectory()) {

                File[] xmlFileList = new File(yarnConfDir).listFiles((dir1, name) -> {
                    if (name.endsWith(".xml")) {
                        return true;
                    }
                    return false;
                });

                if (xmlFileList != null) {
                    for (File xmlFile : xmlFileList) {
                        yarnConf.addResource(xmlFile.toURI().toURL());
                    }
                }
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        haYarnConf(yarnConf);
        return yarnConf;
    }

    /**
     * deal yarn HA conf
     */
    private static Configuration haYarnConf(Configuration yarnConf) {
        Iterator<Map.Entry<String, String>> iterator = yarnConf.iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, String> entry = iterator.next();
            String key = entry.getKey();
            String value = entry.getValue();
            if (key.startsWith("yarn.resourcemanager.hostname.")) {
                String rm = key.substring("yarn.resourcemanager.hostname.".length());
                String addressKey = "yarn.resourcemanager.address." + rm;
                if (yarnConf.get(addressKey) == null) {
                    yarnConf.set(addressKey, value + ":" + YarnConfiguration.DEFAULT_RM_PORT);
                }
            }
        }
        return yarnConf;
    }


    public static org.apache.flink.configuration.Configuration loadFlinkConfig(String confDir) {
        return GlobalConfiguration.loadConfiguration(confDir);
    }

    public static HiveConf loadHiveConfig(String confDir) {
        HiveConf configuration = new HiveConf();
        try {
            File dir = new File(confDir);
            if (dir.exists() && dir.isDirectory()) {

                File[] xmlFileList = new File(confDir).listFiles((dir1, name) -> {
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
        } catch (IOException e) {
            e.printStackTrace();
        }
        return configuration;
    }
}
