package org.weiwan.argus.core;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.weiwan.argus.common.options.OptionParser;
import org.weiwan.argus.common.options.OptionParserV1;
import org.weiwan.argus.core.flink.pub.FlinkContext;
import org.weiwan.argus.core.flink.utils.FlinkContextUtil;
import org.weiwan.argus.core.plugin.ArgusPluginManager;
import org.weiwan.argus.core.pub.api.*;
import org.weiwan.argus.core.pub.config.*;
import org.weiwan.argus.core.start.StartOptions;
import org.weiwan.argus.core.utils.CommonUtil;

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
        CommonUtil.useCommandLogLevel(options.getLogLevel());
        Map<String, Object> optionToMap = optionParser.optionToMap(options);
        //读取job描述文件 json
        String jobConfContent = options.getJobDescJson();
//        Map<String, String> jobMap = YamlUtils.loadYamlStr(jobConfContent);
        Map<String, String> jobMap = JSONObject.parseObject(jobConfContent, Map.class);

        printEnvInfo(optionToMap, jobMap);
        Map<String, Object> tmpObj = new HashMap<>();
        tmpObj.putAll(jobMap);
        tmpObj.putAll(optionToMap);
        ArgusContext argusContext = convertOptionsToContext(options, tmpObj);
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

        //如果没有使用channel,不需要设置
        DataStream channelStream = null;
        if (StringUtils.isNotBlank(channelClassName)) {
            channelStream = channel.channel(readerStream);
        } else {
            channelStream = readerStream;
        }
        DataStreamSink writerSink = writer.writer(channelStream);
        JobExecutionResult execute = env.execute(jobConfig.getStringVal(ArgusKey.KEY_TASK_NAME, "ArgusJob"));

    }


    private static List<URL> getPluginsUrls(String pluginPath) throws MalformedURLException {
        logger.info("plugins path is [{}]", pluginPath);
        File file = new File(pluginPath);
        List<URL> urls = new ArrayList<URL>();
        List<URL> _Urls = findJarsInDir(file);
        urls.addAll(_Urls);
        for (URL url : urls) {
            logger.info(url.getPath());
        }
        return urls;
    }

    private static ArgusContext convertOptionsToContext(StartOptions startOptions, Map<String, Object> taskObj) {
        ArgusContext argusContext = new ArgusContext(startOptions);
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

        return argusContext;
    }

    private static void printEnvInfo(Map<String, Object> optionToMap, Map<String, String> jobMap) {
        logger.debug("Environmental Startup Parameters");
        logger.debug("==============================================================");
        for (String key : optionToMap.keySet()) {
            logger.debug(String.format("key: [%s], value: [%s]", key, optionToMap.get(key)));
        }
        logger.debug("Job Deploy Parameters");
        logger.debug("==============================================================");
        for (String key : jobMap.keySet()) {
            logger.debug(String.format("key: [%s], value: [%s]", key, jobMap.get(key)));
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
