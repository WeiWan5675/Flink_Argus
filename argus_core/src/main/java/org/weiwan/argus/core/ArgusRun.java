package org.weiwan.argus.core;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.weiwan.argus.common.options.OptionParser;
import org.weiwan.argus.common.utils.YamlUtils;
import org.weiwan.argus.core.start.StartOptions;

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
        String jobConf = parse.getJobConf();
        Map<String, String> jobMap = YamlUtils.loadYamlStr(jobConf);
        printEnvInfo(optionToMap, jobMap);


        //解析配置文件

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


}
