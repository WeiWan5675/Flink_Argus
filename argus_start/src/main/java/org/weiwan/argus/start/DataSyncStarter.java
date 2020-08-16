package org.weiwan.argus.start;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.weiwan.argus.common.options.OptionParser;
import org.weiwan.argus.common.utils.FileUtil;
import org.weiwan.argus.common.utils.SystemUtil;
import org.weiwan.argus.common.utils.YamlUtils;
import org.weiwan.argus.core.ArgusKey;
import org.weiwan.argus.core.ArgusRun;
import org.weiwan.argus.core.start.StartOptions;
import org.weiwan.argus.core.utils.CommonUtil;
import org.weiwan.argus.start.enums.RunMode;

import java.io.File;
import java.io.IOException;
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

        OptionParser optionParser = new OptionParser(args);
        StartOptions options = optionParser.parse(StartOptions.class);
        //命令对象 转化成List对象

        options.setLogLevel(CommonUtil.useCommandLogLevel(options.getLogLevel()));
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

        //根据模式不同,组装不同的参数
        if (options.isCmdMode()) {
            //命令行模式
        } else if (options.isExampleMode()) {
            options.setJobDescJson(options.getDefaultJobConf());
        } else {

        }


        String[] argsAll = OptionParser.optionToArgs(options);
        boolean startFlag = false;
        switch (RunMode.valueOf(mode.toLowerCase())) {
            case local:
                logger.info("RunMode:" + RunMode.local.toString());
                startFlag = startFromLocalMode(argsAll, options);
                break;
            case standalone:
                logger.info("RunMode:" + RunMode.standalone.toString());
                startFlag = startFromStandaloneMode(options);
                break;
            case yarn:
                logger.info("RunMode:" + RunMode.yarn.toString());
                startFlag = startFromYarnMode(options);
                break;
            case yarnpre:
                logger.info("RunMode:" + RunMode.yarnpre.toString());
                startFlag = startFromYarnPerMode(options);
                break;
            default:
                logger.info(String.format("No Match RunMode of %s !", mode));
        }

        logger.info(startFlag ? "APP RUN SUCCESS!" : "APP RUN FAILED");


    }


    private static void mergeUserAndDefault(StartOptions options, String userJobConf, String defaultJobConf) {
        logger.info("merge default and job profiles to one profile");

        Map<String, String> defaultJobMap = YamlUtils.loadYamlStr(defaultJobConf);
        Map<String, String> userJobMap = YamlUtils.loadYamlStr(userJobConf);
        logger.info("default profile size: {}", defaultJobMap != null ? defaultJobMap.size() : 0);
        logger.info("job profile size: {}", userJobMap != null ? userJobMap.size() : 0);
        //user overwrite default
        for (String key : userJobMap.keySet()) {
            defaultJobMap.put(key, userJobMap.get(key));
        }
        logger.info("merged profile size: {}", defaultJobMap.size());
        String jobJson = JSONObject.toJSONString(defaultJobMap);
        options.setJobDescJson(jobJson);
    }

    private static void readDefaultJobConf(StartOptions options) throws IOException {
        String argusHome = options.getArgusHome();
        String defaultJobConf = options.getDefaultJobConf();
        String defaultConfStr;
        if (StringUtils.isNotEmpty(defaultJobConf)) {
            defaultConfStr = FileUtil.readFileContent(defaultJobConf);
            logger.info("Specify the default profile, use the specified profile: {}", defaultConfStr);
            options.setDefaultJobConf(defaultJobConf);
        } else {
            String defaultConfDir =
                    argusHome + File.separator + ArgusKey.DEFAULT_CONF_DIR
                            + File.separator + ArgusKey.DEFAULT_CONF_FILENAME;
            defaultConfStr = FileUtil.readFileContent(defaultConfDir);
            logger.info("load Default Profile: {}", defaultConfDir);
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
        return true;
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
        logger.info("argus plugins root dir is: {}", pluginsRootDir);
        //设置插件目录
        options.setPluginsDir(pluginsRootDir);
        String readerPluginDir = defaultMap.getOrDefault(ArgusKey.KEY_ARGUS_PLUGINS_READER_DIR, KEY_READER_PLUGIN_DIR);
        String channelPluginDir = defaultMap.getOrDefault(ArgusKey.KEY_ARGUS_PLUGINS_CHANNEL_DIR, KEY_CHANNEL_PLUGIN_DIR);
        String writerPluginDir = defaultMap.getOrDefault(ArgusKey.KEY_ARGUS_PLUGINS_WRITER_DIR, KEY_WRITER_PLUGIN_DIR);
        if (StringUtils.isEmpty(options.getReaderPluginDir())) {
            if (!FileUtil.isAbsolutePath(readerPluginDir)) {
                readerPluginDir = argusHome + File.separator + DataSyncStarter.KEY_READER_PLUGIN_DIR;
            }
            options.setReaderPluginDir(readerPluginDir);
        } else {
            readerPluginDir = options.getReaderPluginDir();
        }

        logger.info("argus reader plugins dir is: {}", readerPluginDir);

        if (StringUtils.isEmpty(options.getChannelPluginDir())) {
            if (!FileUtil.isAbsolutePath(channelPluginDir)) {
                channelPluginDir = argusHome + File.separator + DataSyncStarter.KEY_CHANNEL_PLUGIN_DIR;
            }
            options.setChannelPluginDir(channelPluginDir);
        } else {
            channelPluginDir = options.getChannelPluginDir();
        }
        logger.info("argus channel plugins dir is: {}", channelPluginDir);

        if (StringUtils.isEmpty(options.getWriterPluginDir())) {
            if (!FileUtil.isAbsolutePath(writerPluginDir)) {
                writerPluginDir = argusHome + File.separator + DataSyncStarter.KEY_WRITER_PLUGIN_DIR;
            }
            options.setWriterPluginDir(writerPluginDir);
        } else {
            channelPluginDir = options.getChannelPluginDir();
        }
        logger.info("argus writer plugins dir is: {}", writerPluginDir);

        //获得flink环境变量
        String flinkHome = SystemUtil.getSystemVar(ArgusKey.KEY_FLINK_HOME);
        String defaultFilnkHome = defaultMap.get(ArgusKey.KEY_FLINK_HOME);
        if (StringUtils.isEmpty(defaultFilnkHome)) {
            //配置文件为空
            options.setFlinkConf(flinkHome + File.separator + "conf");
            logger.debug("get FLINK_HOME From EnvironmentVariable: {}", flinkHome);
        } else {
            //配置文件不为空
            flinkHome = defaultFilnkHome;
            options.setFlinkConf(defaultFilnkHome + File.separator + "conf");
        }
        logger.info("FLINK_HOME path is: {}", flinkHome);
        //获得flink环境变量
        String hadoopHome = SystemUtil.getSystemVar(ArgusKey.KEY_HADOOP_HOME);
        String defaultHadoopHome = defaultMap.get(ArgusKey.KEY_HADOOP_HOME);
        if (StringUtils.isEmpty(defaultHadoopHome)) {
            //配置文件为空
            options.setHadoopConf(hadoopHome + File.separator + "conf");
            logger.debug("get HADOOP_HOME From EnvironmentVariable: {}", hadoopHome);
        } else {
            //配置文件不为空
            hadoopHome = defaultHadoopHome;
            options.setHadoopConf(defaultHadoopHome + File.separator + "etc/hadoop");
        }
        logger.info("HADOOP_HOME path is: {}", hadoopHome);
        //获得flink环境变量
        String hiveHome = SystemUtil.getSystemVar(ArgusKey.KEY_HIVE_HOME);
        String defaultHiveHome = defaultMap.get(ArgusKey.KEY_HIVE_HOME);
        if (StringUtils.isEmpty(defaultHiveHome)) {
            //配置文件为空
            options.setHiveConf(hiveHome + File.separator + "conf");
            logger.debug("get HIVE_HOME From EnvironmentVariable: {}", hiveHome);
        } else {
            //配置文件不为空
            hiveHome = defaultHiveHome;
            options.setHiveConf(defaultHiveHome + File.separator + "conf");
        }
        logger.info("HIVE_HOME path is: {}", hiveHome);

    }

    private static String setArgusHomePath(StartOptions options) {
        String argusHome = options.getArgusHome();

        if (StringUtils.isEmpty(argusHome)) {
            argusHome = SystemUtil.getSystemVar(ArgusKey.KEY_ARGUS_HOME);
        }
        if (StringUtils.isEmpty(argusHome)) {
            logger.warn("the ARUGS_HOME environment variable was not found, use the launcher root directory!");
            logger.warn("use the path of the startup class path as ARGUS_HOME");
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
