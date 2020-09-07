package org.weiwan.argus.core.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.weiwan.argus.core.pub.config.JobConfig;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HadoopUtil {

    private static final Logger LOG = LoggerFactory.getLogger(HadoopUtil.class);
    public static final String AUTHENTICATION_TYPE = "Kerberos";
    public static final String KEY_HADOOP_SECURITY_AUTHORIZATION = "hadoop.security.authorization";
    public static final String KEY_HADOOP_SECURITY_AUTHENTICATION = "hadoop.security.authentication";
    public static final String KEY_DEFAULT_FS = "fs.default.name";
    public static final String KEY_FS_HDFS_IMPL_DISABLE_CACHE = "fs.hdfs.impl.disable.cache";
    public static final String KEY_HA_DEFAULT_FS = "fs.defaultFS";
    public static final String KEY_DFS_NAMESERVICES = "dfs.nameservices";
    public static final String KEY_HADOOP_USER_NAME = "hadoop.user.name";


    public static FileSystem getFileSystem(Configuration configuration) throws IOException {
        FileSystem fileSystem = FileSystem.get(configuration);
        if (isOpenKerberos(configuration)) {
            //开启了kerberos
            return getFsWithKerberos(configuration, configuration.get(KEY_DEFAULT_FS));
        }
        return fileSystem;
    }


    public static Configuration getConfiguration(JobConfig jobConfig) {
        String defaultFs = jobConfig.getStringVal(KEY_DEFAULT_FS);
        Configuration configuration = new Configuration();
        if (StringUtils.isNotEmpty(defaultFs)) {
            configuration.set(KEY_DEFAULT_FS, defaultFs);
        }
        String haDefaultFs = jobConfig.getStringVal(KEY_HA_DEFAULT_FS);
        if (StringUtils.isNotEmpty(haDefaultFs)) {
            configuration.set(KEY_HA_DEFAULT_FS, haDefaultFs);
        }
        String nameservices = jobConfig.getStringVal(KEY_DFS_NAMESERVICES);
        if (StringUtils.isNotEmpty(nameservices)) {
            configuration.set(KEY_DFS_NAMESERVICES, nameservices);
        }
        String hdfsDisableCache = jobConfig.getStringVal(KEY_FS_HDFS_IMPL_DISABLE_CACHE);
        if (StringUtils.isNotEmpty(hdfsDisableCache)) {
            configuration.set(KEY_FS_HDFS_IMPL_DISABLE_CACHE, hdfsDisableCache);
        }

        String hadoopUserName = jobConfig.getStringVal(KEY_HADOOP_USER_NAME);
        if (StringUtils.isEmpty(hadoopUserName)) {
            hadoopUserName = System.getProperty("user.name");
        }
        configuration.set(KEY_HADOOP_USER_NAME, hadoopUserName);
        return configuration;
    }

    private static FileSystem getFsWithKerberos(Configuration configuration, String defaultFs) {
        return getFsWithNoAuth(configuration, defaultFs);
    }

    private static FileSystem getFsWithNoAuth(Configuration configuration, String defaultFs) {
        URI uri = null;
        try {
            String _defaultFs = configuration.get(KEY_HA_DEFAULT_FS);

            if (StringUtils.isNotEmpty(defaultFs) && defaultFs.equalsIgnoreCase(_defaultFs)) {
                //是一样的两个defaultFs
                return FileSystem.get(configuration);
            } else {
                uri = new URI(defaultFs);
            }
            return FileSystem.get(uri, configuration);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return null;
    }


    public static boolean isOpenKerberos(Configuration configuration) {
        String enableAuth = configuration.get(KEY_HADOOP_SECURITY_AUTHORIZATION);
        if (StringUtils.isNotBlank(enableAuth)) {
            if (Boolean.valueOf(enableAuth)) {
                //开启了权限
                String authType = configuration.get(KEY_HADOOP_SECURITY_AUTHENTICATION);
                if (StringUtils.isNotBlank(authType) && AUTHENTICATION_TYPE.equalsIgnoreCase(authType)) {
                    //是Kerberos
                    return true;
                }
            }
        }
        return false;
    }


}