package org.weiwan.argus.core.flink.utils;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.weiwan.argus.core.flink.pub.*;

import java.io.IOException;

/**
 * @Author: xiaozhennan
 * @Date: 2020/4/29 1:42
 * @Package: com.hopson.dc.realtime.java.init
 * @ClassName: FlinkContextUtils
 * @Description:
 **/
public class FlinkContextUtil {

    private static final FlinkLogger logger = FlinkContextUtil.getLogger();
    private static final EnvIniter javaEnvIniter = new JavaEnvIniter();
    private static final EnvIniter scalaEnvIniter = new ScalaEnvIniter();

    public static <T> FlinkContext<T> getContext(final Class<T> envTClass, final String[] args) {
        logger.info("initialize The Flink Environment ---> start");
        FlinkContext<T> flinkContext = null;
        try {
            if (FlinkContains.JAVA_STREAM_ENV == envTClass) {
                StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
                flinkContext = initJavaStreamContext(executionEnvironment, args, envTClass);
            }
            if (FlinkContains.JAVA_BATCH_ENV == envTClass) {
                ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
                flinkContext = initJavaBatchContext(executionEnvironment, args, envTClass);
            }
            if (FlinkContains.SCALA_STREAM_ENV == envTClass) {
                org.apache.flink.streaming.api.scala.StreamExecutionEnvironment executionEnvironment =
                        org.apache.flink.streaming.api.scala.StreamExecutionEnvironment.getExecutionEnvironment();
                flinkContext = initScalaStreamContext(executionEnvironment, args, envTClass);
            }
            if (FlinkContains.SCALA_BATCH_ENV == envTClass) {
                org.apache.flink.api.scala.ExecutionEnvironment executionEnvironment =
                        org.apache.flink.api.scala.ExecutionEnvironment.getExecutionEnvironment();
                flinkContext = initScalaBatchContext(executionEnvironment, args, envTClass);
            }
            logger.info("initialize The Flink Environment ---> end");
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("Failed to initialize Flink environment, please check the log", e);
            flinkContext = null;
        }
        return flinkContext;
    }

    private static <T> FlinkContext<T> initScalaBatchContext(final org.apache.flink.api.scala.ExecutionEnvironment executionEnvironment, String[] args, Class<T> envTClass) {
        FlinkContext context = new FlinkContext(executionEnvironment, envTClass, args);
        scalaEnvIniter.initBatch(context);
        return context;
    }

    private static <T> FlinkContext<T> initScalaStreamContext(final org.apache.flink.streaming.api.scala.StreamExecutionEnvironment executionEnvironment, String[] args, Class<T> envTClass) throws IOException {
        FlinkContext context = new FlinkContext(executionEnvironment, envTClass, args);
        scalaEnvIniter.initStream(context);
        return context;
    }

    private static <T> FlinkContext<T> initJavaBatchContext(final ExecutionEnvironment executionEnvironment, String[] args, Class<T> envTClass) {
        FlinkContext context = new FlinkContext(executionEnvironment, envTClass, args);
        javaEnvIniter.initBatch(context);
        return context;
    }

    private static <T> FlinkContext<T> initJavaStreamContext(final StreamExecutionEnvironment executionEnvironment, String[] args, Class<T> envTClass) throws IOException {
        FlinkContext context = new FlinkContext(executionEnvironment, envTClass, args);
        javaEnvIniter.initStream(context);
        return context;
    }


    /**
     * 根据指定的class打印日志
     *
     * @param tClass
     * @param <T>
     * @return
     */
    public static final <T> FlinkLogger getLogger(final Class<T> tClass) {
        return new FlinkLogger(tClass);
    }

    /**
     * 自动获取当前线程的class打印日志
     *
     * @return
     */
    public static final FlinkLogger getLogger() {
        StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
        StackTraceElement e = stacktrace[2];
        String className = e.getClassName();
        return new FlinkLogger(className);
    }

    public static void main(String[] args) throws IOException {
        FlinkContext<StreamExecutionEnvironment> context = FlinkContextUtil.getContext(FlinkContains.JAVA_STREAM_ENV, args);

        StreamExecutionEnvironment env = context.getEnv();

//        JobExecutionResult execute = env.execute();

    }

    public static void getStreamContext(String jobConfStr) throws IOException {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkContext context = new FlinkContext(executionEnvironment,StreamExecutionEnvironment.class,jobConfStr);
        scalaEnvIniter.initStream(context);
    }
}
