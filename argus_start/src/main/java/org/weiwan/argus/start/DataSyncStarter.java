package org.weiwan.argus.start;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.Context;
import ch.qos.logback.core.FileAppender;
import ch.qos.logback.core.LogbackException;
import ch.qos.logback.core.encoder.Encoder;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.rolling.RollingFileAppender;
import ch.qos.logback.core.rolling.TimeBasedRollingPolicy;
import ch.qos.logback.core.spi.FilterReply;
import ch.qos.logback.core.status.Status;
import com.alibaba.fastjson.JSONObject;
import com.beust.jcommander.JCommander;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.weiwan.argus.common.constans.Constans;
import org.weiwan.argus.common.log.EasyPatternLayout;
import org.weiwan.argus.common.options.OptionParser;
import org.weiwan.argus.common.utils.FileUtil;
import org.weiwan.argus.common.utils.SystemUtil;
import org.weiwan.argus.common.utils.YamlUtils;
import org.weiwan.argus.core.ArgusKey;
import org.weiwan.argus.core.ArgusRun;
import org.weiwan.argus.common.options.OptionParserV1;
import org.weiwan.argus.core.start.StartOptions;
import org.weiwan.argus.core.utils.CommonUtil;
import org.weiwan.argus.start.enums.JobMode;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * @Author: xiaozhennan
 * @Date: 2020/7/14 19:24
 * @Package: PACKAGE_NAME
 * @ClassName: org.weiwan.argus.start.DataSyncStarter
 * @Description:
 **/
public class DataSyncStarter {


    private static final Logger logger = LoggerFactory.getLogger(DataSyncStarter.class);


    private static final String KEY_PLUGINS_DIR = "plugins";

    private static final String KEY_READER_PLUGIN_DIR = "reader";

    private static final String KEY_WRITER_PLUGIN_DIR = "writer";

    private static final String KEY_CHANNEL_PLUGIN_DIR = "channel";


    public static void main(String[] args) throws Exception {
//        args = new String[]{
//                "-mode", "Local",
//                "-aconf", "F:\\Project\\Flink_Argus\\argus_start\\src\\main\\resources\\argus-core.yaml"
//        };


        OptionParser optionParser = new OptionParser(args);
        StartOptions options = optionParser.parse(StartOptions.class);
        //命令对象 转化成List对象
        String mode = options.getMode();
        //设置ArgusHome路径
        setArgusHomePath(options);
        //读取argus-core.yaml配置文件
        readDefaultJobConf(options);
        //设置默认路径
        setDefaultEnvPath(options);
        //读取job配置文件
        readArgusJobConf(options);
        //使用用户配置文件覆盖默认配置文件 形成最终配置文件
        String userJobConf = options.getArgusConf();
        String defaultJobConf = options.getDefaultJobConf();
        mergeUserAndDefault(options, userJobConf, defaultJobConf);


        /**
         * 默认配置文件
         * 1. FlinkEnv配置
         * 2. 日志相关配置
         * 3. Hadoop/Hbase/Hive/HDFS相关配置
         * 4. 默认CHANNEL READER WRITER
         * 5. job-yaml中可以重写argus-cord的相关配置
         */


        //根据模式不同,组装不同的参数
        if (options.isCmdMode()) {
            //命令行模式
        } else if (options.isExampleMode()) {
            options.setJobDescJson(options.getDefaultJobConf());
        } else {

        }

        String[] argsAll = OptionParser.optionToArgs(options);
        boolean startFlag = false;
        switch (JobMode.valueOf(mode.toLowerCase())) {
            case local:
                System.out.println("运行模式:" + JobMode.local.toString());
                startFlag = startFromLocalMode(argsAll, options);
                break;
            case standalone:
                System.out.println("运行模式:" + JobMode.standalone.toString());
                startFlag = startFromStandaloneMode(options);
                break;
            case yarn:
                System.out.println("运行模式:" + JobMode.yarn.toString());
                startFlag = startFromYarnMode(options);
                break;
            case yarnpre:
                System.out.println("运行模式:" + JobMode.yarnpre.toString());
                startFlag = startFromYarnPerMode(options);
                break;
            default:
                System.out.println("没有匹配的运行模式!");
        }

        System.out.println(startFlag);


    }

    private static void mergeUserAndDefault(StartOptions options, String userJobConf, String defaultJobConf) {
        Map<String, String> userJobMap = YamlUtils.loadYamlStr(userJobConf);
        Map<String, String> defaultJobMap = YamlUtils.loadYamlStr(defaultJobConf);
        for (String key : userJobMap.keySet()) {
            defaultJobMap.put(key, userJobMap.get(key));
        }
        String jobJson = JSONObject.toJSONString(defaultJobMap);
        options.setJobDescJson(jobJson);
    }

    private static void readDefaultJobConf(StartOptions options) throws IOException {
        String argusHome = options.getArgusHome();
        String defaultJobConf = options.getDefaultJobConf();
        String defaultConfStr;
        if (StringUtils.isNotEmpty(defaultJobConf)) {
            defaultConfStr = FileUtil.readFileContent(defaultJobConf);
            options.setDefaultJobConf(defaultConfStr);
        } else {
            String defaultConfDir =
                    argusHome + File.separator + ArgusKey.DEFAULT_CONF_DIR
                            + File.separator + ArgusKey.DEFAULT_CONF_FILENAME;
            defaultConfStr = FileUtil.readFileContent(defaultConfDir);
        }
        options.setDefaultJobConf(defaultConfStr);
    }


    private static void readArgusJobConf(StartOptions options) throws IOException {
        //配置文件模式
        String aConfPath = options.getArgusConf();
        String arugsJobContext = FileUtil.readFileContent(aConfPath);
        if (StringUtils.isNotEmpty(arugsJobContext.trim())) {
            options.setArgusConf(arugsJobContext);
        }
    }


    private static boolean startFromYarnPerMode(StartOptions options) {
        return false;
    }

    private static boolean startFromYarnMode(StartOptions options) {

        return false;
    }

    private static boolean startFromLocalMode(String[] argsAll, StartOptions options) throws Exception {


        ArgusRun.main(argsAll);


        return false;
    }

    private static void setDefaultEnvPath(StartOptions options) {

        String argusHome = options.getArgusHome();
        String defaultJobContent = options.getDefaultJobConf();
        Map<String, String> defaultMap = YamlUtils.loadYamlStr(defaultJobContent);
        String pluginsRootDir = options.getPluginsDir();
        String defaultPluginsDir = defaultMap.getOrDefault(ArgusKey.KEY_ARGUS_PLUGINS_DIR, KEY_PLUGINS_DIR);
        if (!FileUtil.isAbsolutePath(defaultPluginsDir)) {
            //是相对路径,需要拼接argusHome
            defaultPluginsDir = argusHome + File.separator + defaultPluginsDir;
        }
        if (StringUtils.isEmpty(pluginsRootDir)) {
            pluginsRootDir = defaultPluginsDir;
        }


        //设置插件目录
        options.setPluginsDir(pluginsRootDir);
        String readerPluginDir = defaultMap.getOrDefault(ArgusKey.KEY_ARGUS_PLUGINS_READER_DIR, KEY_READER_PLUGIN_DIR);
        String channelPluginDir = defaultMap.getOrDefault(ArgusKey.KEY_ARGUS_PLUGINS_CHANNEL_DIR, KEY_CHANNEL_PLUGIN_DIR);
        String writerPluginDir = defaultMap.getOrDefault(ArgusKey.KEY_ARGUS_PLUGINS_WRITER_DIR, KEY_WRITER_PLUGIN_DIR);
        if (StringUtils.isEmpty(options.getReaderPluginDir())) {
            if (FileUtil.isAbsolutePath(readerPluginDir)) {
                options.setReaderPluginDir(readerPluginDir);
            } else {
                options.setReaderPluginDir(argusHome + File.separator + DataSyncStarter.KEY_READER_PLUGIN_DIR);
            }
        }
        if (StringUtils.isEmpty(options.getChannelPluginDir())) {
            if (FileUtil.isAbsolutePath(channelPluginDir)) {
                options.setChannelPluginDir(channelPluginDir);
            } else {
                options.setChannelPluginDir(argusHome + File.separator + DataSyncStarter.KEY_CHANNEL_PLUGIN_DIR);
            }
        }
        if (StringUtils.isEmpty(options.getWriterPluginDir())) {
            if (FileUtil.isAbsolutePath(writerPluginDir)) {
                options.setWriterPluginDir(writerPluginDir);
            } else {
                options.setWriterPluginDir(argusHome + File.separator + DataSyncStarter.KEY_WRITER_PLUGIN_DIR);
            }
        }


        //获得flink环境变量
        String flinkHome = SystemUtil.getSystemVar(ArgusKey.KEY_FLINK_HOME);
        String defaultFilnkHome = defaultMap.get(ArgusKey.KEY_FLINK_HOME);
        if (StringUtils.isEmpty(defaultFilnkHome)) {
            //配置文件为空
            options.setFlinkConf(flinkHome + File.separator + "conf");
        } else {
            //配置文件不为空
            options.setFlinkConf(defaultFilnkHome + File.separator + "conf");
        }


        //获得flink环境变量
        String hadoopHome = SystemUtil.getSystemVar(ArgusKey.KEY_HADOOP_HOME);
        String defaultHadoopHome = defaultMap.get(ArgusKey.KEY_HADOOP_HOME);
        if (StringUtils.isEmpty(defaultHadoopHome)) {
            //配置文件为空
            options.setHadoopConf(hadoopHome + File.separator + "conf");
        } else {
            //配置文件不为空
            options.setHadoopConf(defaultHadoopHome + File.separator + "etc/hadoop");
        }

        //获得flink环境变量
        String hiveHome = SystemUtil.getSystemVar(ArgusKey.KEY_HIVE_HOME);
        String defaultHiveHome = defaultMap.get(ArgusKey.KEY_HIVE_HOME);
        if (StringUtils.isEmpty(defaultHiveHome)) {
            //配置文件为空
            options.setHiveConf(hiveHome + File.separator + "conf");
        } else {
            //配置文件不为空
            options.setHiveConf(defaultHiveHome + File.separator + "conf");
        }

    }

    private static String setArgusHomePath(StartOptions options) {
        String argusHome = options.getArgusHome();

        if (StringUtils.isEmpty(argusHome)) {
            argusHome = SystemUtil.getSystemVar(ArgusKey.KEY_ARGUS_HOME);
        }
        if (StringUtils.isEmpty(argusHome)) {
            logger.warn("The ARUGS_HOME environment variable was not found, use the launcher root directory!");
            //获得当前启动类jar包得实际地址 $ARGUS_`HOME/lib
            String appPath = CommonUtil.getAppPath(DataSyncStarter.class);
            File file = new File(appPath);
            argusHome = file.getParent();
        }
        options.setArgusHome(argusHome);
        logger.info(String.format("ARGUS_HOME is [%s]", argusHome));
        return argusHome;
    }

    private static boolean startFromStandaloneMode(StartOptions options) {
        return false;
    }


}


//转化脚本启动的options为Main方法可以识别的参数
//pluginPath
//配置分两种

/**
 * -1. 解析参数
 * 0. 初始化 hadoop yarn flink 配置文件夹
 * 1. 判断启动模式 cliMode  CMD CONF 分为cmd模式  和conf配置文件模式
 * 2. 根据启动模式的不同,获取对应的插件/运行类信息
 * 4. 根据job类型 获取不同的任务提交信息
 * 5. 本地提交模式 不需要额外的信息
 * 6. yarn | yarnPer | 需要获得 jar信息远程连接信息 并且打包
 * 6. 提交任务
 * 8. 关闭资源
 */


//启动方式分为两种 1. CMD方式  通过识别options中-cmd

//配置文件的方式  支持的参数很多 支持覆盖flink-conf的配置  设置任务级别的配置


//CMD的方式  支持简单的参数配置  支持简单的

//FlinkArgus -aconf "./argus-conf.yaml" -fconf "./flink-conf.yaml" -queue -yq

//提交任务就可以了  不需要考虑其它的  初始化参数  插件加载  都放到任务里去

//一种是插件配置

//一种是mysql
//ReaderPluginName
//WriterPluginName

//ReaderPluginClassName
//WriterPluginClassName


//Flink自身运行的一些参数

//任务的配置文件

//
