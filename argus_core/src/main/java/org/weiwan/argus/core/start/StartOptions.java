package org.weiwan.argus.core.start;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.weiwan.argus.common.options.Option;
import org.weiwan.argus.common.options.OptionField;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/14 20:04
 * @Package: org.weiwan.argus.common.options
 * @ClassName: DemoOptions
 * @Description:
 **/
public class StartOptions implements Serializable {

    @Parameter(names = "-cmd", description = "client mode")
    private boolean cmdMode = false;

    @Parameter(names = {"-mode", "-m"}, required = true, description = "Flink task runing mode")
    private String mode = "Local";

    @Parameter(names = {"-flinkConf", "-fconf"}, description = "Flink conf file path")
    private String flinkConf;

    @Parameter(names = {"-hadoopConf", "-hconf"}, description = "Hadoop and yarn conf file path")
    private String hadoopConf;

    @Parameter(names = {"yarnConf", "yconf"}, description = "Yarn conf path")
    private String yarnConf;

    public boolean isCmdMode() {
        return cmdMode;
    }

    @Parameter(names = {"-hiveConf"}, description = "hive conf file path")
    private String hiveConf;

    @Parameter(names = {"-queue", "-yq"}, description = "Yarn queue name")
    private String yarnQueue = "default";

    @Parameter(names = {"-p", "-parallelism"}, description = "job parallelism setting")
    private Integer parallelism = 1;

    @Parameter(names = {"-sp"}, description = "Save point path")
    private String savePointPath;

    @Parameter(names = "-pd", description = "Plugins jar path")
    private String pluginsDir;

    @Parameter(names = "-rd", description = "reader plugins path")
    private String readerPluginDir;

    @Parameter(names = "-wd", description = "writer plugin path")
    private String writerPluginDir;

    @Parameter(names = "-cd", description = "channel plugin path")
    private String channelPluginDir;

    @Parameter(names = "-appHome", description = "argus root path")
    private String argusHome;
    //内部使用

    @Parameter(names = "-jobDescJson", description = "argus job desc josn")
    private String jobDescJson;

    @Parameter(names = {"-argusConf", "-aconf", "-jobConf"}, description = "Argus Job Desc File Path")
    private String argusConf;

    @Parameter(names = "-defaultArgusConf", description = "default Argus job desc josn")
    private String defaultJobConf;

    @Parameter(names = "-exampleMode", description = "run example!")
    private boolean exampleMode = false;

    @Parameter(names = "appId",description = "application job id")
    private String appId;

    @Parameter(names = "-logLevel", description = "log level setting")
    private String logLevel = "info";

    @Parameter(names = "--help", help = true, order = 5)
    private boolean help;


    @DynamicParameter(names = "-D", description = "Dynamic parameters go here")
    private Map<String, String> params = new HashMap<String, String>();

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

    public String getHiveConf() {
        return hiveConf;
    }

    public void setHiveConf(String hiveConf) {
        this.hiveConf = hiveConf;
    }

    public String getYarnQueue() {
        return yarnQueue;
    }

    public void setYarnQueue(String yarnQueue) {
        this.yarnQueue = yarnQueue;
    }

    public Integer getParallelism() {
        return parallelism;
    }

    public void setParallelism(Integer parallelism) {
        this.parallelism = parallelism;
    }

    public String getSavePointPath() {
        return savePointPath;
    }

    public void setSavePointPath(String savePointPath) {
        this.savePointPath = savePointPath;
    }

    public String getPluginsDir() {
        return pluginsDir;
    }

    public void setPluginsDir(String pluginsDir) {
        this.pluginsDir = pluginsDir;
    }

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

    public String getJobDescJson() {
        return jobDescJson;
    }

    public void setJobDescJson(String jobDescJson) {
        this.jobDescJson = jobDescJson;
    }

    public String getArgusConf() {
        return argusConf;
    }

    public void setArgusConf(String argusConf) {
        this.argusConf = argusConf;
    }

    public String getDefaultJobConf() {
        return defaultJobConf;
    }

    public void setDefaultJobConf(String defaultJobConf) {
        this.defaultJobConf = defaultJobConf;
    }

    public boolean isExampleMode() {
        return exampleMode;
    }

    public void setExampleMode(boolean exampleMode) {
        this.exampleMode = exampleMode;
    }

    public String getLogLevel() {
        return logLevel;
    }

    public void setLogLevel(String logLevel) {
        this.logLevel = logLevel;
    }

    public boolean isHelp() {
        return help;
    }

    public void setHelp(boolean help) {
        this.help = help;
    }

    public Map<String, String> getParams() {
        return params;
    }

    public void setParams(Map<String, String> params) {
        this.params = params;
    }

    public String getYarnConf() {
        return yarnConf;
    }

    public void setYarnConf(String yarnConf) {
        this.yarnConf = yarnConf;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }
}
