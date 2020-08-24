package org.weiwan.argus.core.utils;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.weiwan.argus.core.constants.ArgusConstans;
import org.weiwan.argus.core.start.StartOptions;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.weiwan.argus.core.constants.ArgusConstans.*;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/31 10:09
 * @Package: org.weiwan.argus.core.utils
 * @ClassName: HadoopConfigLoader
 * @Description:
 **/
public class ClusterConfigLoader {


    private static Configuration hadoopConfiguration;
    private static org.apache.flink.configuration.Configuration flinkConfiguration;
    private static YarnConfiguration yarnConfiguration;

    private static HiveConf hiveConfiguration;

    public static void main(String[] args) {
//        Configuration configuration = ClusterConfigLoader.loadHadoopConfig("F:\\hadoop-common-2.6.0-bin\\etc\\hadoop");
//        System.out.println(configuration);
    }


    public static Configuration loadHadoopConfig(StartOptions options) {
        if (hadoopConfiguration == null) {
            hadoopConfiguration = new Configuration();
            String hadoopConf = options.getHadoopConf();

            try {
                File dir = new File(hadoopConf);
                if (dir.exists() && dir.isDirectory()) {

                    File[] xmlFileList = new File(hadoopConf).listFiles((dir1, name) -> {
                        if (name.endsWith(".xml")) {
                            return true;
                        }
                        return false;
                    });

                    if (xmlFileList != null) {
                        for (File xmlFile : xmlFileList) {
                            hadoopConfiguration.addResource(xmlFile.toURI().toURL());
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return hadoopConfiguration;
    }


    public static YarnConfiguration loadYarnConfig(StartOptions options) {
        if (yarnConfiguration == null) {
            yarnConfiguration = new YarnConfiguration();
            try {
                String yarnConf = options.getYarnConf();
                File dir = new File(yarnConf);
                if (dir.exists() && dir.isDirectory()) {

                    File[] xmlFileList = new File(yarnConf).listFiles((dir1, name) -> {
                        if (name.endsWith(".xml")) {
                            return true;
                        }
                        return false;
                    });

                    if (xmlFileList != null) {
                        for (File xmlFile : xmlFileList) {
                            yarnConfiguration.addResource(xmlFile.toURI().toURL());
                        }
                    }
                }

            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            haYarnConf(yarnConfiguration);
        }
        return yarnConfiguration;
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


    public static org.apache.flink.configuration.Configuration loadFlinkConfig(StartOptions options) {
        if (flinkConfiguration == null) {
            String flinkConf = options.getFlinkConf();
            String yarnConf = options.getYarnConf();
            String yarnQueue = options.getYarnQueue();
            String pluginLoadMode = options.getPluginLoadMode();
            String appId = options.getAppId();

            flinkConfiguration = StringUtils.isEmpty(flinkConf) ? new org.apache.flink.configuration.Configuration() : GlobalConfiguration.loadConfiguration(flinkConf);
            if (StringUtils.isNotBlank(yarnQueue)) {
                ConfigOption<String> APPLICATION_QUEUE =
                        key("yarn.application.queue")
                                .stringType()
                                .noDefaultValue()
                                .withDescription("The YARN queue on which to put the current pipeline.");
                flinkConfiguration.setString(APPLICATION_QUEUE, yarnQueue);
            }
            if (StringUtils.isNotBlank(appId)) {
                ConfigOption<String> APPLICATION_NAME =
                        key("yarn.application.name")
                                .stringType()
                                .noDefaultValue()
                                .withDescription("A custom name for your YARN application.");
                flinkConfiguration.setString(APPLICATION_NAME, appId);
            }
            if (StringUtils.isNotBlank(yarnConf)) {
                flinkConfiguration.setString("fs.hdfs.hadoopconf", yarnConf);
            }
            ConfigOption<String> CLASSLOADER_RESOLVE_ORDER = ConfigOptions.key("classloader.resolve-order").defaultValue("child-first").withDescription("Defines the class resolution strategy when loading classes from user code, meaning whether to first check the user code jar (\"child-first\") or the application classpath (\"parent-first\"). The default settings indicate to load classes first from the user code jar, which means that user code jars can include and load different dependencies than Flink uses (transitively).");
            if (CLASS_PATH_PLUGIN_LOAD_MODE.equalsIgnoreCase(pluginLoadMode)) {
                flinkConfiguration.setString(CLASSLOADER_RESOLVE_ORDER, CLASSLOADER_CHILD_FIRST);
            } else {
                flinkConfiguration.setString(CLASSLOADER_RESOLVE_ORDER, CLASSLOADER_PARENT_FIRST);
            }

            flinkConfiguration.setString(ArgusConstans.FLINK_PLUGIN_LOAD_MODE_KEY, pluginLoadMode);
        }
        return flinkConfiguration;
    }

    public static HiveConf loadHiveConfig(StartOptions options) {
        if (hiveConfiguration == null) {
            hiveConfiguration = new HiveConf();
            String hiveConf = options.getHiveConf();
            try {
                File dir = new File(hiveConf);
                if (dir.exists() && dir.isDirectory()) {

                    File[] xmlFileList = new File(hiveConf).listFiles((dir1, name) -> {
                        if (name.endsWith(".xml")) {
                            return true;
                        }
                        return false;
                    });

                    if (xmlFileList != null) {
                        for (File xmlFile : xmlFileList) {
                            hiveConfiguration.addResource(xmlFile.toURI().toURL());
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return hiveConfiguration;
    }
}
