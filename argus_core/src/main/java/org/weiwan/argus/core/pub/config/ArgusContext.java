package org.weiwan.argus.core.pub.config;

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
    private Map<String, Object> startupParameters;

    public ArgusContext(Map<String, Object> startupParameters) {
        this.startupParameters = startupParameters;
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
}
