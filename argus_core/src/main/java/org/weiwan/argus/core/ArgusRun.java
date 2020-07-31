package org.weiwan.argus.core;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.weiwan.argus.common.options.OptionParser;
import org.weiwan.argus.common.utils.YamlUtils;
import org.weiwan.argus.core.flink.pub.FlinkContext;
import org.weiwan.argus.core.flink.utils.FlinkContextUtil;
import org.weiwan.argus.core.plugin.ArgusPluginManager;
import org.weiwan.argus.core.pub.api.*;
import org.weiwan.argus.core.pub.config.*;
import org.weiwan.argus.core.pub.pojo.DataRecord;
import org.weiwan.argus.core.start.StartOptions;
import org.weiwan.argus.core.utils.ClusterConfigLoader;

import java.io.File;
import java.io.FilenameFilter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/14 16:29
 * @Package: org.weiwan.argus
 * @ClassName: ArgusRun
 * @Description: Argus入口类，读取配置，提交JOB等
 **/
public class ArgusRun {

    private static final Logger logger = LoggerFactory.getLogger(ArgusRun.class);

    public static void main(String[] args) throws Exception {
        //args的参数已经经过了启动器处理,所以可以直接使用OptionsParser进行解析工作
        OptionParser optionParser = new OptionParser(args);
        StartOptions options = optionParser.parse(StartOptions.class);

        Map<String, Object> optionToMap = optionParser.optionToMap(options, StartOptions.class);

        //读取job描述文件 json
        String jobConfContent = options.getJobConf();
        Map<String, String> jobMap = YamlUtils.loadYamlStr(jobConfContent);
        printEnvInfo(optionToMap, jobMap);
        Map<String, Object> tmpObj = new HashMap<>();
        tmpObj.putAll(jobMap);
        ArgusContext argusContext = convertOptionsToContext(optionToMap, tmpObj);
        //任务总体描述
        JobConfig jobConfig = argusContext.getJobConfig();
        //flink环境相关描述
        FlinkEnvConfig flinkEnvConfig = argusContext.getFlinkEnvConfig();
        FlinkContext<StreamExecutionEnvironment> streamContext = FlinkContextUtil.getStreamContext(flinkEnvConfig);
        StreamExecutionEnvironment env = streamContext.getEnv();
        //reader插件相关描述
        ReaderConfig readerConfig = jobConfig.getReaderConfig();
        //channel插件相关描述
        ChannelConfig channelConfig = jobConfig.getChannelConfig();
        //writer插件相关描述
        WriterConfig writerConfig = jobConfig.getWriterConfig();

        //获得插件目录
        List<URL> rUrls = getPluginsUrls(options.getReaderPluginDir());
        List<URL> cUrls = getPluginsUrls(options.getChannelPluginDir());
        List<URL> wUrls = getPluginsUrls(options.getWriterPluginDir());

        String readerClassName = readerConfig.getStringVal(ArgusKey.KEY_READER_CLASS_NAME);
        String channelClassName = channelConfig.getStringVal(ArgusKey.KEY_CHANNEL_CLASS_NAME);
        String writerClassName = writerConfig.getStringVal(ArgusKey.KEY_WRITER_CLASS_NAME);

        ArgusPluginManager argusPluginManager = new ArgusPluginManager(env, argusContext);

        ArgusReader reader = argusPluginManager.loadReaderPlugin(rUrls, readerClassName);

        ArgusChannel channel = argusPluginManager.loadChannelPlugin(cUrls, channelClassName);

        ArgusWriter writer = argusPluginManager.loadWriterPlugin(wUrls, writerClassName);

        DataStream readerStream = reader.reader();
        DataStream channelStream = channel.channel(readerStream);
        DataStreamSink writerSink = writer.writer(channelStream);
        JobExecutionResult execute = env.execute(jobConfig.getStringVal("flink.task.name", "ArgusJob"));

    }


    private static List<URL> getPluginsUrls(String pluginPath) throws MalformedURLException {
        logger.info("plugins path is [{}]", pluginPath);
        File file = new File(pluginPath);
        List<URL> urls = new ArrayList<URL>();
        List<URL> _Urls = findJarsInDir(file);
        urls.addAll(_Urls);
        for (URL url : urls) {
            System.out.println(url);
        }
        return urls;
    }

    private static ArgusContext convertOptionsToContext(Map<String, Object> optionToMap, Map<String, Object> taskObj) {
        ArgusContext argusContext = new ArgusContext(optionToMap);
        FlinkEnvConfig flinkEnvConfig = new FlinkEnvConfig(new HashMap<>());
        JobConfig jobConfig = new JobConfig(taskObj);

        for (String taskKey : taskObj.keySet()) {
            if (taskKey.startsWith("flink.task")) {
                //flink的配置
                flinkEnvConfig.setVal(taskKey, taskObj.get(taskKey));
            }
        }
        argusContext.setFlinkEnvConfig(flinkEnvConfig);
        argusContext.setJobConfig(jobConfig);


        //设置三大环境变量
        /**
         * 1. Hadoop
         * 2. Flink
         * 3. Yarn
         */
        Map<String, Object> startArgs = argusContext.getStartupParameters();

        return argusContext;
    }

    private static void printEnvInfo(Map<String, Object> optionToMap, Map<String, String> jobMap) {
        System.out.println("Environmental Startup Parameters");
        System.out.println("==============================================================");
        for (String key : optionToMap.keySet()) {
            System.out.println(String.format("key: [%s], value: [%s]", key, optionToMap.get(key)));
        }
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println("Job Deploy Parameters");
        System.out.println("==============================================================");
        for (String key : jobMap.keySet()) {
            System.out.println(String.format("key: [%s], value: [%s]", key, jobMap.get(key)));
        }
    }


    public static List<URL> findJarsInDir(File dir) throws MalformedURLException {
        List<URL> urlList = new ArrayList<>();

        if (dir.exists() && dir.isDirectory()) {
            File[] jarFiles = dir.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    return name.toLowerCase().endsWith(".jar");
                }
            });

            for (File jarFile : jarFiles) {
                urlList.add(jarFile.toURI().toURL());
            }

        }

        return urlList;
    }


}
