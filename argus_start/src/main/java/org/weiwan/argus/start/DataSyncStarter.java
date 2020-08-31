package org.weiwan.argus.start;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.deployment.executors.RemoteExecutor;
import org.apache.flink.client.program.*;
import org.apache.flink.configuration.*;
import org.apache.flink.core.execution.DefaultExecutorServiceLoader;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.weiwan.argus.common.options.OptionParser;
import org.weiwan.argus.common.utils.FileUtil;
import org.weiwan.argus.common.utils.SystemUtil;
import org.weiwan.argus.common.utils.YamlUtils;
import org.weiwan.argus.core.ArgusKey;
import org.weiwan.argus.core.ArgusRun;
import org.weiwan.argus.core.constants.ArgusConstans;
import org.weiwan.argus.core.start.StartOptions;
import org.weiwan.argus.core.utils.ClusterConfigLoader;
import org.weiwan.argus.core.utils.CommonUtil;
import org.weiwan.argus.start.cluster.ClusterClientFactory;
import org.weiwan.argus.core.enums.RunMode;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
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

    public static final String KEY_PLUGINS_DIR = "plugins";
    public static final String KEY_READER_PLUGIN_DIR = "reader";
    public static final String KEY_WRITER_PLUGIN_DIR = "writer";
    public static final String KEY_CHANNEL_PLUGIN_DIR = "channel";


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
        String pluginRoot = options.getPluginsDir();
        String libDir = options.getLibDir();
        String extLibDir = options.getExtLibDir();
        String readerPluginDir = options.getReaderPluginDir();
        String channelPluginDir = options.getChannelPluginDir();
        String writerPluginDir = options.getWriterPluginDir();
        List<URL> urlList = findLibJar(libDir, extLibDir, pluginRoot, readerPluginDir, channelPluginDir, writerPluginDir);
        String coreJarFileName = findCoreJarFile(libDir);
        File coreJarFile = new File(libDir + File.separator + coreJarFileName);

        boolean startFlag = false;
        switch (RunMode.valueOf(mode.toLowerCase())) {
            case local:
                logger.info("RunMode:" + RunMode.local.toString());
                startFlag = startFromLocalMode(argsAll, options);
                break;
            case standalone:
                logger.info("RunMode:" + RunMode.standalone.toString());
                startFlag = startFromStandaloneMode(options, coreJarFile, urlList, argsAll);
                break;
            case yarn:
                logger.info("RunMode:" + RunMode.yarn.toString());
                startFlag = startFromYarnMode(options, coreJarFile, urlList, argsAll);
                break;
            case yarnper:
                logger.info("RunMode:" + RunMode.yarnper.toString());
                startFlag = startFromYarnPerMode(options, coreJarFile, urlList, argsAll);
                break;

            case application:
                logger.info("RunMode:" + RunMode.application.toString());
                startFlag = startFromApplicationMode(options, coreJarFile, urlList, argsAll);
            default:
                logger.info(String.format("No Match RunMode of %s !", mode));
        }

        logger.info(startFlag ? "APP RUN SUCCESS!" : "APP RUN FAILED");


    }

    private static boolean startFromApplicationMode(StartOptions options, File coreJarFile, List<URL> urlList, String[] argsAll) throws ProgramInvocationException {
        PackagedProgram packagedProgram = buildProgram(options, coreJarFile, urlList, argsAll);
        Configuration configuration = ClusterConfigLoader.loadFlinkConfig(options);
        List<String> classpaths = new ArrayList<>();
        for (URL classpath : urlList) {
            classpaths.add(classpath.toString());
        }

        String flinkConfDir = options.getFlinkConf();
        String yarnConfDir = options.getYarnConf();

        Configuration flinkConfiguration = ClusterConfigLoader.loadFlinkConfig(options);
        if (org.apache.commons.lang.StringUtils.isNotBlank(flinkConfDir)) {
            try {
                FileSystem.initialize(flinkConfiguration);

                YarnConfiguration yarnConf = ClusterConfigLoader.loadYarnConfig(options);
                YarnClient yarnClient = YarnClient.createYarnClient();
                yarnClient.init(yarnConf);
                yarnClient.start();
                ApplicationId applicationId;

                if (org.apache.commons.lang.StringUtils.isEmpty(options.getAppId())) {
                    applicationId = ClusterClientFactory.getAppIdFromYarn(yarnClient, options);
                    if (applicationId == null || org.apache.commons.lang.StringUtils.isEmpty(applicationId.toString())) {
                        throw new RuntimeException("No flink session found on yarn cluster.");
                    }
                } else {
                    applicationId = ConverterUtils.toApplicationId(options.getAppId());
                }
                configuration.set(ArgusConstans.CLASSPATHS, classpaths);
                configuration.setString(DeploymentOptions.TARGET, RemoteExecutor.NAME);
                HighAvailabilityMode highAvailabilityMode = HighAvailabilityMode.fromConfig(configuration);
                if (highAvailabilityMode.equals(HighAvailabilityMode.ZOOKEEPER) && applicationId != null) {
                    configuration.setString(HighAvailabilityOptions.HA_CLUSTER_ID, applicationId.toString());
                }
                executeProgram(configuration, packagedProgram);
                return false;
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
        return false;
    }

    private static List<URL> findLibJar(String... libDirs) throws MalformedURLException {
        List<URL> urls = new ArrayList<>();

        if (libDirs.length < 1) {
            return urls;
        }

        for (String dir : libDirs) {
            File libDir = new File(dir);
            if (libDir.exists() && libDir.isDirectory()) {
                List<URL> jarsInDir = SystemUtil.findJarsInDir(libDir);
                urls.addAll(jarsInDir);
            }
        }
        return urls;
    }

    private static String findCoreJarFile(String libDir) throws FileNotFoundException {
        String coreJarFileName = null;
        File libPath = new File(libDir);
        if (libPath.exists() && libPath.isDirectory()) {
            File[] jarFiles = libPath.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    return name.toLowerCase().startsWith("argus_core") && name.toLowerCase().endsWith(".jar");
                }
            });

            if (jarFiles != null && jarFiles.length > 0) {
                coreJarFileName = jarFiles[0].getName();
            }
        }

        if (org.apache.commons.lang.StringUtils.isEmpty(coreJarFileName)) {
            throw new FileNotFoundException("Can not find core jar file in path:" + libDir);
        }
        return coreJarFileName;
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


    /**
     * 独占模式,单独一个YarnSession
     *
     * @param options
     * @param coreJarFile
     * @param urlList
     * @param argsAll
     * @return
     */
    private static boolean startFromYarnPerMode(StartOptions options, File
            coreJarFile, List<URL> urlList, String[] argsAll) throws Exception {
//        String libJar = options.getLibDir();
//        if (StringUtils.isBlank(libJar)) {
//            throw new IllegalArgumentException("per-job mode must have flink lib path!");
//        }
//        addMonitorToArgs(argsAll, "");
//        PerJobSubmitter.submit(options, new JobGraph(), coreJarFile, urlList, argsAll);
        return true;
    }

/**
 * 1. 判断是否是yarn模式
 * 2. 如果是yarn模式,检查classpath配置
 * 3. 如果是hdfs的classpath,检查hdfsclasspath是否正常
 * 4. 使用hdfsclasspath
 * 5. hdfs classpath hdfs://flink/flink_argus/lib
 */

    /**
     * 提交到YarnSession中,默认的YarnSession名称为Flink Session Cluster
     *
     * @param options
     * @param coreJarFile
     * @param urlList
     * @param argsAll
     * @return
     * @throws Exception
     */
    private static boolean startFromYarnMode(StartOptions options, File coreJarFile, List<URL> urlList, String[]
            argsAll) throws Exception {
        ClusterClient clusterClient = ClusterClientFactory.createClusterClient(options);
        String webInterfaceURL = clusterClient.getWebInterfaceURL();
        addMonitorToArgs(argsAll, webInterfaceURL);
        JobGraph jobGraph = buildJobGraph(options, coreJarFile, urlList, argsAll);
        ClientUtils.submitJob(clusterClient, jobGraph);
        return true;
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

        //设置依赖目录
        String libDir = options.getLibDir();
        if (StringUtils.isEmpty(libDir)) {
            options.setLibDir(argusHome + File.separator + "lib");
        }
        String extLibDir = options.getExtLibDir();
        if (StringUtils.isEmpty(extLibDir)) {
            options.setExtLibDir(argusHome + File.separator + "extLib");
        }

        //设置扩展依赖目录

        logger.info("argus plugins root dir is: {}", pluginsRootDir);
        //设置插件目录
        options.setPluginsDir(pluginsRootDir);
        String readerPluginDir = defaultMap.getOrDefault(ArgusKey.KEY_ARGUS_PLUGINS_READER_DIR, KEY_READER_PLUGIN_DIR);
        String channelPluginDir = defaultMap.getOrDefault(ArgusKey.KEY_ARGUS_PLUGINS_CHANNEL_DIR, KEY_CHANNEL_PLUGIN_DIR);
        String writerPluginDir = defaultMap.getOrDefault(ArgusKey.KEY_ARGUS_PLUGINS_WRITER_DIR, KEY_WRITER_PLUGIN_DIR);
        if (StringUtils.isEmpty(options.getReaderPluginDir())) {
            if (!FileUtil.isAbsolutePath(readerPluginDir)) {
                readerPluginDir = argusHome + File.separator +
                        DataSyncStarter.KEY_PLUGINS_DIR + File.separator +
                        DataSyncStarter.KEY_READER_PLUGIN_DIR;
            }
            options.setReaderPluginDir(readerPluginDir);
        } else {
            readerPluginDir = options.getReaderPluginDir();
        }

        logger.info("argus reader plugins dir is: {}", readerPluginDir);

        if (StringUtils.isEmpty(options.getChannelPluginDir())) {
            if (!FileUtil.isAbsolutePath(channelPluginDir)) {
                channelPluginDir = argusHome + File.separator +
                        DataSyncStarter.KEY_PLUGINS_DIR + File.separator +
                        DataSyncStarter.KEY_CHANNEL_PLUGIN_DIR;
            }
            options.setChannelPluginDir(channelPluginDir);
        } else {
            channelPluginDir = options.getChannelPluginDir();
        }
        logger.info("argus channel plugins dir is: {}", channelPluginDir);

        if (StringUtils.isEmpty(options.getWriterPluginDir())) {
            if (!FileUtil.isAbsolutePath(writerPluginDir)) {
                writerPluginDir = argusHome + File.separator +
                        DataSyncStarter.KEY_PLUGINS_DIR + File.separator +
                        DataSyncStarter.KEY_WRITER_PLUGIN_DIR;
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
        options.setFlinkHome(flinkHome);
        options.setFlinkLibDir(flinkHome + File.separator + "lib");
        logger.info("FLINK_HOME path is: {}", flinkHome);
        setDefaultHadoopEnv(options, defaultMap);
        setDefaultHiveEnv(options, defaultMap);
        setDefaultYarnEnv(options, defaultMap);

        String hadoopUserName = SystemUtil.getSystemVar("HADOOP_USER_NAME");
        String defaultHadoopUserName = defaultMap.get("HADOOP_USER_NAME");
        if (!defaultHadoopUserName.equalsIgnoreCase(hadoopUserName)) {
            SystemUtil.setSystemVar("HADOOP_USER_NAME", defaultHadoopUserName);
            hadoopUserName = defaultHadoopUserName;
        }
        options.setHadoopUserName(hadoopUserName);
    }

    private static void setDefaultYarnEnv(StartOptions options, Map<String, String> defaultMap) {
        String yarnHome = SystemUtil.getSystemVar(ArgusKey.KEY_HIVE_HOME);
        String defaultYarnHome = defaultMap.get(ArgusKey.KEY_HIVE_HOME);
        if (StringUtils.isEmpty(defaultYarnHome)) {
            //配置文件为空
            options.setYarnConf(yarnHome + File.separator + "conf");
            logger.debug("get YARN_HOME From EnvironmentVariable: {}", yarnHome);
        } else {
            //配置文件不为空
            yarnHome = defaultYarnHome;
            if (StringUtils.isEmpty(yarnHome))
                options.setYarnConf(yarnHome + File.separator + "conf");
        }
        if (StringUtils.isNotEmpty(yarnHome)) {
            options.setYarnHome(yarnHome);
            logger.info("YARN_HOME path is: {}", yarnHome);
        } else {
            logger.debug("YARN_HOME path is null");
        }
    }

    private static void setDefaultHiveEnv(StartOptions options, Map<String, String> defaultMap) {
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
        if (StringUtils.isNotEmpty(hiveHome)) {
            options.setHiveHome(hiveHome);
            logger.info("HIVE_HOME path is: {}", hiveHome);
        } else {
            logger.debug("HIVE_HOME path is null");
        }
    }

    private static void setDefaultHadoopEnv(StartOptions options, Map<String, String> defaultMap) {
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
        if (StringUtils.isNotEmpty(hadoopHome)) {
            options.setHadoopHome(hadoopHome);
            logger.info("HADOOP_HOME path is: {}", hadoopHome);
        } else {
            logger.debug("HADOOP_HOME path is null");
        }
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

    private static boolean startFromStandaloneMode(StartOptions options, File
            coreJarFile, List<URL> urlList, String... argsAll) throws Exception {
        ClusterClient clusterClient = ClusterClientFactory.createStandaloneClient(options);
        String webInterfaceURL = clusterClient.getWebInterfaceURL();
        String[] args = addMonitorToArgs(argsAll, webInterfaceURL);
        JobGraph jobGraph = buildJobGraph(options, coreJarFile, urlList, args);
        ClientUtils.submitJob(clusterClient, jobGraph);
        return true;
    }

    public static String[] addMonitorToArgs(String[] argsAll, String rul) {
        String[] args = new String[argsAll.length + 1];
        System.out.println(args.length);
        for (int i = 0; i < argsAll.length; i++) {
            args[i] = argsAll[i];
        }
        args[args.length - 1] = "-monitor";
        args[args.length - 2] = rul;
        return args;
    }


    private static JobGraph buildJobGraph(StartOptions options, File coreJarFile, List<URL> urls, String[]
            argsAll) throws Exception {
        String flinkConf = options.getFlinkConf();
        Configuration configuration = ClusterConfigLoader.loadFlinkConfig(options);
        PackagedProgram program = PackagedProgram.newBuilder()
                .setJarFile(coreJarFile)
                .setUserClassPaths(urls)
                .setEntryPointClassName(ArgusConstans.ARGUS_CORE_RUN_CLASS)
                .setConfiguration(configuration)
                .setArguments(argsAll)
                .build();
        return PackagedProgramUtils.createJobGraph(program, configuration, options.getParallelism(), false);
    }


    private static PackagedProgram buildProgram(StartOptions options, File coreJarFile, List<URL> urls, String[]
            argsAll) throws ProgramInvocationException {
        Configuration configuration = ClusterConfigLoader.loadFlinkConfig(options);
        return PackagedProgram.newBuilder()
                .setJarFile(coreJarFile)
                .setUserClassPaths(urls)
                .setEntryPointClassName(ArgusConstans.ARGUS_CORE_RUN_CLASS)
                .setConfiguration(configuration)
                .setArguments(argsAll)
                .build();
    }

    private static void executeProgram(final Configuration configuration, final PackagedProgram program) throws
            ProgramInvocationException {
        ClientUtils.executeProgram(new DefaultExecutorServiceLoader(), configuration, program, false, false);
    }


}


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
