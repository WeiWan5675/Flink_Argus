package org.weiwan.argus.start;

import org.apache.commons.codec.Charsets;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.weiwan.argus.common.constans.Constans;
import org.weiwan.argus.common.exception.ArgusCommonException;
import org.weiwan.argus.common.utils.SystemUtil;
import org.weiwan.argus.core.ArgusRun;
import org.weiwan.argus.common.options.OptionParser;
import org.weiwan.argus.core.start.StartOptions;
import org.weiwan.argus.core.utils.CommonUtil;
import org.weiwan.argus.start.enums.JobMode;

import java.io.File;
import java.io.FileInputStream;
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
    private static final String KEY_FLINK_HOME = "FLINK_HOME";
    private static final String KEY_ARGUS_HOME = "ARGUS_HOME";
    private static final String KEY_HADOOP_HOME = "HADOOP_HOME";


    private static final String KEY_PLUGINS_DIR = "plugins";

    private static final String KEY_READER_PLUGIN_DIR = "reader";

    private static final String KEY_WRITER_PLUGIN_DIR = "writer";

    private static final String KEY_CHANNEL_PLUGIN_DIR = "channel";


    public static void main(String[] args) throws Exception {
        args = new String[]{
                "-mode" ,"Local",
                "-aconf","G:\\Project\\Flink_Argus\\argus_start\\src\\main\\resources\\argus-default.yaml"
        };
        OptionParser optionParser = new OptionParser(args);
        StartOptions options = optionParser.parse(StartOptions.class);
        //命令对象 转化成List对象

        String mode = options.getMode();
        setDefaultEnvPath(options);

        //根据模式不同,组装不同的参数
        if (options.isCmdMode()) {
            //命令行模式
        } else {
            readingArgusConfig(options);
        }
        Map<String, Object> optionMap = optionParser.optionToMap(options, StartOptions.class);

        String[] argsAll;
        argsAll = convertMap2Args(optionMap);
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

    private static String[] convertMap2Args(Map<String, Object> optionMap) {
        List<String> argsList = new ArrayList<>();
        for (String key : optionMap.keySet()) {
            String cmdKey = Constans.SIGN_HORIZONTAL + key;
            String var = String.valueOf(optionMap.get(key));
            if (StringUtils.isNotEmpty(var)) {
                argsList.add(cmdKey);
                argsList.add(var);
            }
        }

        String[] argsAll = argsList.toArray(new String[argsList.size()]);
        return argsAll;
    }


    private static void readingArgusConfig(StartOptions options) throws IOException {
        //配置文件模式
        String aConfPath = options.getArgusConf();
        //读取配置文件  转化成json对象
        File file = new File(aConfPath);
        if (!file.exists() || file.isDirectory()) {
            logger.error(String.format("Configuration file does not exist, please check, PATH:[%s]", file.getAbsolutePath()));
            throw new ArgusCommonException("The configuration file does not exist, please check the configuration file path!");
        }
        FileInputStream in = new FileInputStream(file);
        byte[] filecontent = new byte[(int) file.length()];
        in.read(filecontent);
        String arugsJobContext = new String(filecontent, Charsets.UTF_8.name());

        if (StringUtils.isNotEmpty(arugsJobContext.trim())) {
            options.setJobConf(arugsJobContext);
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
        //转化脚本启动的options为Main方法可以识别的参数
        //pluginPath
        //配置分两种

        /**
         *-1. 解析参数
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


        return false;
    }

    private static void setDefaultEnvPath(StartOptions options) {


        String argusHome = getArgusHomePath(options);

        String pluginsRootDir = options.getPluginsDir();
        if (StringUtils.isEmpty(pluginsRootDir)) {
            pluginsRootDir = argusHome + File.separator + KEY_PLUGINS_DIR;
        }

        //获得flink环境变量
        String flinkHome = SystemUtil.getSystemVar(DataSyncStarter.KEY_FLINK_HOME);
        //设置Flink目录
        options.setFlinkConf(flinkHome + File.separator + "conf");
        //获得hadoop环境变量
        String hadoopHome = SystemUtil.getSystemVar(DataSyncStarter.KEY_HADOOP_HOME);
        //设置Hadoop目录
        options.setHadoopConf(hadoopHome + File.separator + "conf");
        //设置插件目录
        options.setPluginsDir(pluginsRootDir);
        if(StringUtils.isEmpty(options.getReaderPluginDir())){
            options.setReaderPluginDir(pluginsRootDir + File.separator + DataSyncStarter.KEY_READER_PLUGIN_DIR);
        }
        if(StringUtils.isEmpty(options.getWriterPluginDir())){
            options.setWriterPluginDir(pluginsRootDir + File.separator + DataSyncStarter.KEY_WRITER_PLUGIN_DIR);
        }
        if(StringUtils.isEmpty(options.getChannelPluginDir())){
            options.setChannelPluginDir(pluginsRootDir + File.separator + DataSyncStarter.KEY_CHANNEL_PLUGIN_DIR);
        }


    }

    private static String getArgusHomePath(StartOptions options) {
        String argusHome = options.getArgusHome();

        if (StringUtils.isEmpty(argusHome)) {
            argusHome = SystemUtil.getSystemVar(DataSyncStarter.KEY_ARGUS_HOME);
        }
        if (StringUtils.isEmpty(argusHome)) {
            logger.warn("The ARUGS_HOME environment variable was not found, use the launcher root directory!");
            //获得当前启动类jar包得实际地址 $ARGUS_`HOME/lib
            String appPath = CommonUtil.getAppPath(DataSyncStarter.class);
            File file = new File(appPath);
            argusHome = file.getParent();
        }
        logger.info(String.format("ARGUS_HOME is [%s]", argusHome));
        return argusHome;
    }

    private static boolean startFromStandaloneMode(StartOptions options) {

        return false;
    }


}
