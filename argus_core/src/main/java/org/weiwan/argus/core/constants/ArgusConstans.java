package org.weiwan.argus.core.constants;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

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



}
