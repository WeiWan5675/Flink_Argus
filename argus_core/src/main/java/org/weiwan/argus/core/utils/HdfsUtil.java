package org.weiwan.argus.core.utils;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.*;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.weiwan.argus.common.utils.DateUtils;
import org.weiwan.argus.core.flink.pub.FlinkLogger;
import org.weiwan.argus.core.pub.output.hdfs.ColumnType;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class HdfsUtil {


    private static final String AUTHENTICATION_TYPE = "Kerberos";
    private static final String KEY_HADOOP_SECURITY_AUTHORIZATION = "hadoop.security.authorization";
    private static final String KEY_HADOOP_SECURITY_AUTHENTICATION = "hadoop.security.authentication";
    private static final String KEY_DEFAULT_FS = "fs.default.name";
    private static final String KEY_FS_HDFS_IMPL_DISABLE_CACHE = "fs.hdfs.impl.disable.cache";
    private static final String KEY_HA_DEFAULT_FS = "fs.defaultFS";
    private static final String KEY_DFS_NAMESERVICES = "dfs.nameservices";
    private static final String KEY_HADOOP_USER_NAME = "hadoop.user.name";
    private static final Logger LOG = LoggerFactory.getLogger(HdfsUtil.class);


    /**
     * 移动文件块到target
     *
     * @param src       源文件
     * @param dst       目标文件
     * @param fs        文件操作对下
     * @param overwrite 是否覆盖
     * @throws IOException 操作文件时可能抛出此异常
     */
    public static void moveBlockToTarget(Path src, Path dst, FileSystem fs, boolean overwrite) throws IOException {
        if (overwrite) {
            try {
                boolean exists = fs.exists(dst);
                if (exists) {
                    fs.delete(dst, true);
                }
                fs.rename(src, dst);
            } catch (IOException e) {
                e.printStackTrace();
                throw e;
            }
        } else {
            throw new RuntimeException("target block exists");
        }
    }


    public static void deleteFile(Path dst, FileSystem fs, boolean recursive) {
        try {
            if (fs.exists(dst)) {
                fs.delete(dst, recursive);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static FileSystem getFileSystem(Configuration configuration, String defaultFs) throws IOException {
        //
        FileSystem fileSystem = FileSystem.get(configuration);

        if (isOpenKerberos(configuration)) {
            //开启了kerberos
            return getFsWithKerberos(configuration, defaultFs);
        }

        return fileSystem;
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


    public static void setHadoopUserName(Configuration conf) {
        String hadoopUserName = conf.get(KEY_HADOOP_USER_NAME);
        if (org.apache.commons.lang.StringUtils.isEmpty(hadoopUserName)) {
            return;
        }

        try {
//            String ticketCachePath = conf.get("hadoop.security.kerberos.ticket.cache.path");
//            UserGroupInformation ugi = UserGroupInformation.getBestUGI(ticketCachePath, hadoopUserName);
//            UserGroupInformation.setLoginUser(ugi);
        } catch (Exception e) {
            LOG.warn("Set hadoop user name error:", e);
        }
    }


}