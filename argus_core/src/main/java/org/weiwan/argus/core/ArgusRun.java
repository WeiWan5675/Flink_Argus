package org.weiwan.argus.core;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.weiwan.argus.common.options.OptionParser;
import org.weiwan.argus.common.utils.YamlUtils;
import org.weiwan.argus.core.pub.api.*;
import org.weiwan.argus.core.pub.config.*;
import org.weiwan.argus.core.pub.pojo.DataRecord;
import org.weiwan.argus.core.start.StartOptions;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/14 16:29
 * @Package: org.weiwan.argus
 * @ClassName: ArgusRun
 * @Description: Argus入口类，读取配置，提交JOB等
 **/
public class ArgusRun {


    public static void main(String[] args) throws Exception {
        //args的参数已经经过了启动器处理,所以可以直接使用OptionsParser进行解析工作
        OptionParser optionParser = new OptionParser(args);
        StartOptions parse = optionParser.parse(StartOptions.class);

        Map<String, Object> optionToMap = optionParser.optionToMap(parse, StartOptions.class);

        //读取job描述文件 json
        String jobConf = parse.getJobConf();
        Map<String, Object> jobMap = YamlUtils.loadYamlStr(jobConf);

        printEnvInfo(optionToMap, jobMap);

        ArgusContext context = new ArgusContext(optionToMap, jobMap);


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //任务总体描述
        JobConfig jobConfig = context.getJobConfig();
        //flink环境相关描述
        FlinkEnvConfig flinkEnvConfig = context.getFlinkEnvConfig();
        //reader插件相关描述
        ReaderConfig readerConfig = jobConfig.getReaderConfig();
        //channel插件相关描述
        ChannelConfig channelConfig = jobConfig.getChannelConfig();
        //writer插件相关描述
        WriterConfig writerConfig = jobConfig.getWriterConfig();

        String readerName = readerConfig.getReaderName();

        String channelName = channelConfig.getChannleName();

        String writerName = writerConfig.getWriterName();


        Class<?> aClass = Class.forName(readerName);
        Constructor<?> constructor = aClass.getConstructor(StreamExecutionEnvironment.class, ArgusContext.class);
        ArgusReader argusReader = (ArgusReader) constructor.newInstance(env, context);
        ArgusInputFormatSource<DataRecord<?>> source = argusReader.reader();
        DataStream<DataRecord<?>> stream = env.addSource(source);


        Class<?> bClass = Class.forName(channelName);
        Constructor<?> bconstructor = bClass.getConstructor(StreamExecutionEnvironment.class, ArgusContext.class);
        ArgusChannel argusChannel = (ArgusChannel) bconstructor.newInstance(env, context);
        ArgusChannelHandler dataStream2 = argusChannel.channel();
        DataStream<DataRecord<?>> mapStream = stream.map(dataStream2);


        Class<?> cClass = Class.forName(writerName);
        Constructor<?> cconstructor = cClass.getConstructor(StreamExecutionEnvironment.class, ArgusContext.class);
        ArgusWriter argusWriter = (ArgusWriter) cconstructor.newInstance(env, context);
        ArgusOutputFormatSink<DataRecord<?>> dataStream1 = argusWriter.writer();
        DataStreamSink<DataRecord<?>> dataRecordDataStreamSink = mapStream.addSink(dataStream1);

        JobExecutionResult execute = env.execute("");
    }

    private static void printEnvInfo(Map<String, Object> optionToMap, Map<String, Object> jobMap) {
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


}
