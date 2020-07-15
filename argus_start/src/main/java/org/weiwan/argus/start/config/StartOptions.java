package org.weiwan.argus.start.config;

import org.weiwan.argus.common.options.Option;
import org.weiwan.argus.common.options.OptionField;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/14 20:04
 * @Package: org.weiwan.argus.common.options
 * @ClassName: DemoOptions
 * @Description:
 **/
@Option("startOptions")
public class StartOptions {

    @OptionField(
            defaultValue = "Local",
            required = true,
            oweKeys = {"mode", "m"},
            description = "运行模式")
    private String mode;

    @OptionField(
            defaultValue = "flink-conf.yaml",
            oweKeys = {"flinkConf", "fconf"},
            description = "Flink default conf file")
    private String flinkConf;

    @OptionField(
            defaultValue = "hadoop-core.properties",
            oweKeys = {"hadoopConf", "hconf"},
            description = "Hadoop default conf file")
    private String hadoopConf;

    @OptionField(
            defaultValue = "argus-default.yaml",
            required = true,
            oweKeys = {"argusConf", "aconf"},
            description = "Argus conf file")
    private String argusConf;


    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public String getFlinkConf() {
        return flinkConf;
    }

    public void setFlinkConf(String flinkConf) {
        this.flinkConf = flinkConf;
    }

    public String getHadoopConf() {
        return hadoopConf;
    }

    public void setHadoopConf(String hadoopConf) {
        this.hadoopConf = hadoopConf;
    }

    public String getArgusConf() {
        return argusConf;
    }

    public void setArgusConf(String argusConf) {
        this.argusConf = argusConf;
    }
}
