package org.weiwan.argus.core.pub.config;

import java.util.Map;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/17 16:43
 * @Package: org.weiwan.argus.core.pub.config
 * @ClassName: ArgusContextConfig
 * @Description:
 **/
public class ArgusContext {

    private JobConfig JobConfig;
    private FlinkEnvConfig flinkEnvConfig;




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
