package org.weiwan.argus.init;

import org.apache.commons.lang3.StringUtils;
import org.weiwan.argus.common.utils.SystemUtil;
import org.weiwan.argus.core.ArgusRun;
import org.weiwan.argus.common.options.OptionParser;
import org.weiwan.argus.core.start.StartOptions;
import org.weiwan.argus.core.utils.CommonUtil;
import org.weiwan.argus.init.enums.JobMode;

import java.io.File;
import java.util.Map;


/**
 * @Author: xiaozhennan
 * @Date: 2020/7/14 19:24
 * @Package: PACKAGE_NAME
 * @ClassName: org.weiwan.argus.start.DataSyncStarter
 * @Description:
 **/
public class DataSyncStarter {

    private static final String KEY_FLINK_HOME = "FLINK_HOME";
    private static final String KEY_ARGUS_HOME = "ARGUS_HOME";
    private static final String KEY_HADOOP_HOME = "HADOOP_HOME";


    private static final String KEY_PLUGINS_DIR = "plugins";

    private static final String KEY_READER_PLUGIN_DIR = "reader";

    private static final String KEY_WRITER_PLUGIN_DIR = "writer";

    private static final String KEY_CHANNEL_PLUGIN_DIR = "channel";


    public static void main(String[] args) throws Exception {
        args = new String[]{
                "--mode", "Local",
                "--aconf", "argus-default.yaml",
                "-cmd"
//                "--hconf","hadoop-cmd.yaml"

        };
        OptionParser optionParser = new OptionParser(args);
        StartOptions options = optionParser.parse(StartOptions.class);
        //命令对象 转化成List对象

        Map<String, Object> optionMap = optionParser.optionToMap(options, StartOptions.class);

        for (String opKey : optionMap.keySet()) {
            System.out.println(optionMap.get(opKey));
        }

        String mode = options.getMode();
        setDefaultEnvPath(options);
        //根据模式不同,组装不同的参数
        if (options.isCmdMode()) {
            //命令行模式
        } else {
            //配置文件模式
            String argusConf = options.getArgusConf();
            //读取配置文件  转化成json对象
        }

        boolean startFlag = false;
        switch (JobMode.valueOf(mode)) {
            case Local:
                System.out.println("运行模式:" + JobMode.Local.toString());
                startFlag = startFromLocalMode(options);
                break;
            case Standalone:
                System.out.println("运行模式:" + JobMode.Standalone.toString());
                startFlag = startFromStandaloneMode(options);
                break;
            case Yarn:
                System.out.println("运行模式:" + JobMode.Yarn.toString());
                startFlag = startFromYarnMode(options);
                break;
            case YarnPer:
                System.out.println("运行模式:" + JobMode.YarnPer.toString());
                startFlag = startFromYarnPerMode(options);
                break;
            default:
                System.out.println("没有匹配的运行模式!");
        }

        System.out.println(startFlag);


    }

    private static boolean startFromYarnPerMode(StartOptions options) {
        return false;
    }

    private static boolean startFromYarnMode(StartOptions options) {

        return false;
    }

    private static boolean startFromLocalMode(StartOptions options) throws Exception {
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

        ArgusRun.main(new String[]{});
        return false;
    }

    private static void setDefaultEnvPath(StartOptions options) {
        String argusHome = options.getArgusHome();
        if (StringUtils.isEmpty(argusHome)) {
            argusHome = SystemUtil.getSystemVar(DataSyncStarter.KEY_ARGUS_HOME);
        }
        String pluginsRootDir = options.getPluginsDir();
        if (StringUtils.isEmpty(pluginsRootDir)) {
            pluginsRootDir = argusHome + File.separator + KEY_PLUGINS_DIR;
        }


        String property = System.getProperty("java.class.path");
        String property1 = System.getProperty("usr.dir");
        String appPath = CommonUtil.getAppPath(DataSyncStarter.class);
        System.out.println(property);
        System.out.println(property1);
        System.out.println(appPath);
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
        options.setReaderPluginDir(pluginsRootDir + File.separator + DataSyncStarter.KEY_READER_PLUGIN_DIR);
        options.setWriterPluginDir(pluginsRootDir + File.separator + DataSyncStarter.KEY_WRITER_PLUGIN_DIR);
        options.setChannelPluginDir(pluginsRootDir + File.separator + DataSyncStarter.KEY_CHANNEL_PLUGIN_DIR);
    }

    private static boolean startFromStandaloneMode(StartOptions options) {

        return false;
    }


}
