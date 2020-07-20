package org.weiwan.argus.core.flink.pub;


import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.weiwan.argus.common.enums.ArgusExceptionEnum;
import org.weiwan.argus.common.exception.ArgusCommonException;
import org.weiwan.argus.common.utils.DateUtils;
import org.weiwan.argus.core.flink.utils.FlinkContextUtil;
import org.weiwan.argus.core.pub.config.FlinkEnvConfig;

import java.util.Date;
import java.util.Map;

/**
 * @Author: xiaozhennan
 * @Date: 2020/4/29 1:43
 * @Package: com.hopson.dc.realtime.java.init
 * @ClassName: FlinkContext
 * @Description:
 **/
public class FlinkContext<T> {

    public static final FlinkLogger logger = FlinkContextUtil.getLogger(FlinkContext.class);

    private T env;
    private String[] args;
    private Class<T> envClass;
    private String contextContent;
    private FlinkConfig<String, String> flinkConfig;
    private FlinkEnvConfig flinkEnvConfig;

    public FlinkContext() {

    }

    public FlinkContext(T executionEnvironment, Class<T> envTClass, String[] args) {
        this.env = executionEnvironment;
        this.envClass = envTClass;
        this.args = args;
    }

    public FlinkContext(T executionEnvironment) {
    }

    public FlinkContext(T executionEnvironment, Class<T> streamExecutionEnvironmentClass, String contextArgs) {

    }

    public FlinkContext(T executionEnvironment, FlinkEnvConfig flinkEnvConfig) {
        this.env = executionEnvironment;
        this.flinkEnvConfig = flinkEnvConfig;
    }

    public T getEnv() {
        return (T) env;
    }

    public void setArgs(String[] args) {
        this.args = args;
    }

    public String[] getArgs() {
        return args;
    }


    public Class<T> getEnvClass() {
        return envClass;
    }

    public <E> void addFlinkConfig(final Map<String, String> toMap) {
        this.flinkConfig = new FlinkConfig(toMap);
    }

    public FlinkConfig<String, String> getFlinkConfig() {
        return this.flinkConfig;
    }


    public JobExecutionResult execute(final String taskName) throws Exception {
        try {
            return executeTask(taskName);
        } catch (Exception e) {
            logger.error("启动Flink程序失败! 请检查日志!", e);
        }
        return null;
    }

    public JobExecutionResult execute() throws Exception {
        try {
            String taskName = flinkConfig.getVar(FlinkContains.FLINK_TASK_NAME);
            if (StringUtils.isEmpty(taskName)) {
                taskName = FlinkContains.FLINK_TASK_NAME_DEFAULT_PREFIX + DateUtils.getDateStr(new Date());
            }
            return executeTask(taskName);
        } catch (Exception e) {
            logger.error("启动Flink程序失败! 请检查日志!", e);
            throw e;
        }
    }

    private JobExecutionResult executeTask(final String taskName) throws Exception {
        if (FlinkContains.JAVA_STREAM_ENV == this.envClass) {
            StreamExecutionEnvironment waitEnv = (StreamExecutionEnvironment) env;
            return waitEnv.execute(taskName);
        }
        if (FlinkContains.JAVA_BATCH_ENV == this.envClass) {
            ExecutionEnvironment waitEnv = (ExecutionEnvironment) env;
            return waitEnv.execute(taskName);
        }
        if (FlinkContains.SCALA_STREAM_ENV == this.envClass) {
            org.apache.flink.streaming.api.scala.StreamExecutionEnvironment waitEnv = (org.apache.flink.streaming.api.scala.StreamExecutionEnvironment) env;
            return waitEnv.execute(taskName);
        }
        if (FlinkContains.SCALA_BATCH_ENV == this.envClass) {
            org.apache.flink.api.scala.ExecutionEnvironment waitEnv = (org.apache.flink.api.scala.ExecutionEnvironment) env;
            return waitEnv.execute(taskName);
        }
        throw new ArgusCommonException(ArgusExceptionEnum.FAILURE);
    }


    public String getContextContent() {
        return contextContent;
    }

    public void setContextContent(String contextContent) {
        this.contextContent = contextContent;
    }

    public Map<String, Object> getFlinkEnvConfig() {
        return flinkEnvConfig.getAll();
    }
}
