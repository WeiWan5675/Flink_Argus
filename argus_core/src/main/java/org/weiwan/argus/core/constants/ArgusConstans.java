package org.weiwan.argus.core.constants;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.util.List;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/15 19:18
 * @Package: org.weiwan.argus.core.constants
 * @ClassName: ArgusConstans
 * @Description:
 **/
@SuppressWarnings("ALL")
public class ArgusConstans {


    public static final String STAR_SYMBOL = "*";
    public static final String POINT_SYMBOL = ".";
    public static final String EQUAL_SYMBOL = "=";
    public static final String SINGLE_QUOTE_MARK_SYMBOL = "'";
    public static final String DOUBLE_QUOTE_MARK_SYMBOL = "\"";
    public static final String COMMA_SYMBOL = ",";
    public static final String HENG_GANG = "-";
    public static final String SINGLE_SLASH_SYMBOL = "/";
    public static final String DOUBLE_SLASH_SYMBOL = "//";

    public static final String LEFT_PARENTHESIS_SYMBOL = "(";
    public static final String RIGHT_PARENTHESIS_SYMBOL = ")";

    public static final String KEY_HTTP = "http";

    public static final String PROTOCOL_HTTP = "http://";
    public static final String PROTOCOL_HTTPS = "https://";
    public static final String PROTOCOL_HDFS = "hdfs://";
    public static final String PROTOCOL_JDBC_MYSQL = "jdbc:mysql://";

    public static final String SYSTEM_PROPERTIES_KEY_OS = "os.name";
    public static final String SYSTEM_PROPERTIES_KEY_USER_DIR = "user.dir";
    public static final String SYSTEM_PROPERTIES_KEY_JAVA_VENDOR = "java.vendor";
    public static final String SYSTEM_PROPERTIES_KEY_FILE_ENCODING = "file.encoding";

    public static final String OS_WINDOWS = "windows";

    public static final String SHIP_FILE_PLUGIN_LOAD_MODE = "shipfile";
    public static final String CLASS_PATH_PLUGIN_LOAD_MODE = "classpath";

    public static final String CLASSLOADER_CHILD_FIRST = "child-first";
    public static final String CLASSLOADER_PARENT_FIRST = "parent-first";
    public static final String ARGUS_CORE_RUN_CLASS = "org.weiwan.argus.core.ArgusRun";


    public static final String FLINK_CHECKPOINT_INTERVAL_KEY = "flink.checkpoint.interval";

    public static final String FLINK_CHECKPOINT_TIMEOUT_KEY = "flink.checkpoint.timeout";

    public static final String YARN_RESOURCE_MANAGER_WEBAPP_ADDRESS_KEY = "yarn.resourcemanager.webapp.address";


    public static final ConfigOption<String> FLINK_PLUGIN_LOAD_MODE_KEY = ConfigOptions
            .key("pluginLoadMode")
            .stringType()
            .defaultValue(ArgusConstans.CLASS_PATH_PLUGIN_LOAD_MODE)
            .withDescription("The config parameter defining YarnPer mode plugin loading method." +
                    "classpath: The plugin package is not uploaded when the task is submitted. " +
                    "The plugin package needs to be deployed in the pluginRoot directory of the yarn-node node, but the task starts faster" +
                    "shipfile: When submitting a task, upload the plugin package under the pluginRoot directory to deploy the plug-in package. " +
                    "The yarn-node node does not need to deploy the plugin package. " +
                    "The task startup speed depends on the size of the plugin package and the network environment.");
    /**
     * A list of jar files that contain the user-defined function (UDF) classes and all classes used from within the UDFs.
     */
    public static final ConfigOption<List<String>> JARS =
            key("pipeline.jars")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription("A semicolon-separated list of the jars to package with the job jars to be sent to the" +
                            " cluster. These have to be valid paths.");
    /**
     * A list of URLs that are added to the classpath of each user code classloader of the program.
     * Paths must specify a protocol (e.g. file://) and be accessible on all nodes
     */
    public static final ConfigOption<List<String>> CLASSPATHS =
            key("pipeline.classpaths")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription("A semicolon-separated list of the classpaths to package with the job jars to be sent to" +
                            " the cluster. These have to be valid URLs.");


}
