package org.weiwan.argus;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.weiwan.argus.pub.api.BaseReader;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/14 16:29
 * @Package: org.weiwan.argus
 * @ClassName: ArgusRun
 * @Description: Argus入口类，读取配置，提交JOB等
 **/
public class ArgusRun {


    public static void main(String[] args) throws Exception {
//        testMain();
        realMain();
    }

    public static void realMain() {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();


    }


    public static void testMain() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Object> source = env.addSource(new RichSourceFunction<Object>() {
            @Override
            public void run(SourceContext<Object> ctx) throws Exception {
                while (true) {
                    ctx.collect("123456");
                }
            }

            @Override
            public void cancel() {

            }
        });


        DataStream<Object> map = source.map(new MapFunction<Object, Object>() {
            @Override
            public Object map(Object value) throws Exception {
                String s = (String) value;
                return s + "1234214124";
            }
        });


        map.addSink(new SinkFunction<Object>() {

            @Override
            public void invoke(Object value, Context context) throws Exception {
                System.out.println(value);
            }
        }).setParallelism(1);


        env.execute();
    }
}
