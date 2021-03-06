package org.weiwan.argus.core.pub.config;

import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.weiwan.argus.core.start.StartOptions;

import java.io.Serializable;
import java.util.Map;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/17 16:43
 * @Package: org.weiwan.argus.core.pub.config
 * @ClassName: ArgusContextConfig
 * @Description:
 **/
public class ArgusContext implements Serializable {

    private JobConfig JobConfig;
    private FlinkEnvConfig flinkEnvConfig;
    private StartOptions startOptions;

    public ArgusContext(StartOptions startOptions) {
        this.startOptions = startOptions;
    }

    public ArgusContext() {

    }


    public JobConfig getJobConfig() {
        return JobConfig;
    }

    public void setJobConfig(JobConfig jobConfig) {
        JobConfig = jobConfig;
    }

    public FlinkEnvConfig getFlinkEnvConfig() {
        return flinkEnvConfig;
    }

    public void setFlinkEnvConfig(FlinkEnvConfig flinkEnvConfig) {
        this.flinkEnvConfig = flinkEnvConfig;
    }

    public StartOptions getStartOptions() {
        return startOptions;
    }

    public void setStartOptions(StartOptions startOptions) {
        this.startOptions = startOptions;
    }
}
