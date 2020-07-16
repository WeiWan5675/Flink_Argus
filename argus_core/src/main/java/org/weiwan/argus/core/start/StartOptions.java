package org.weiwan.argus.core.start;

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
            defaultValue = "false",
            hasArg = false,
            description = "client mode",
            oweKeys = {"cmd"})
    private boolean cmdMode;

    @OptionField(
            defaultValue = "Local",
            required = true,
            oweKeys = {"mode", "m"},
            description = "运行模式")
    private String mode;

    @OptionField(
            oweKeys = {"flinkConf", "fconf"},
            description = "Flink conf file path")
    private String flinkConf;

    @OptionField(
            oweKeys = {"hadoopConf", "hconf"},
            description = "Hadoop and yarn conf file path")
    private String hadoopConf;

    @OptionField(
            oweKeys = {"argusConf", "aconf","job"},
            description = "Argus conf file")
    private String argusConf;

    @OptionField(
            oweKeys = {"queue", "yq"},
            description = "Yarn queue name")
    private String yarnQueue = "default";

    @OptionField(
            oweKeys = {"p"},
            description = "job parallelism setting")
    private String parallelism = "1";

    @OptionField(
            oweKeys = "sp",
            description = "Save point path")
    private String savePointPath;


    @OptionField(
            oweKeys = "pd",
            description = "plugins path")
    private String pluginsDir;

    @OptionField(
            oweKeys = "rd",
            description = "reader plugin path")
    private String readerPluginDir;

    @OptionField(
            oweKeys = "wd",
            description = "writer plugin path")
    private String writerPluginDir;

    @OptionField(
            oweKeys = "cd",
            description = "channel plugin path")
    private String channelPluginDir;


    @OptionField(
            oweKeys = "rd",
            description = "argus root path")
    private String argusHome;



    public String getReaderPluginDir() {
        return readerPluginDir;
    }



    public void setReaderPluginDir(String readerPluginDir) {
        this.readerPluginDir = readerPluginDir;
    }

    public String getWriterPluginDir() {
        return writerPluginDir;
    }

    public void setWriterPluginDir(String writerPluginDir) {
        this.writerPluginDir = writerPluginDir;
    }

    public String getChannelPluginDir() {
        return channelPluginDir;
    }

    public void setChannelPluginDir(String channelPluginDir) {
        this.channelPluginDir = channelPluginDir;
    }

    public String getArgusHome() {
        return argusHome;
    }

    public void setArgusHome(String argusHome) {
        this.argusHome = argusHome;
    }

    public String getPluginsDir() {
        return pluginsDir;
    }

    public void setPluginsDir(String pluginsDir) {
        this.pluginsDir = pluginsDir;
    }

    public boolean isCmdMode() {
        return cmdMode;
    }

    public void setCmdMode(boolean cmdMode) {
        this.cmdMode = cmdMode;
    }

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

    public String getYarnQueue() {
        return yarnQueue;
    }

    public void setYarnQueue(String yarnQueue) {
        this.yarnQueue = yarnQueue;
    }

    public String getParallelism() {
        return parallelism;
    }

    public void setParallelism(String parallelism) {
        this.parallelism = parallelism;
    }

    public String getSavePointPath() {
        return savePointPath;
    }

    public void setSavePointPath(String savePointPath) {
        this.savePointPath = savePointPath;
    }
}
