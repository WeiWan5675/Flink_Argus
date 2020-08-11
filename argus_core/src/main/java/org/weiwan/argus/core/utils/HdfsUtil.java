package org.weiwan.argus.core.utils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.*;
import org.weiwan.argus.common.utils.DateUtils;
import org.weiwan.argus.core.pub.output.hdfs.ColumnType;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class HdfsUtil {

    public static final String NULL_VALUE = "\\N";

    public static Object string2col(String str, String type, SimpleDateFormat customDateFormat) {
        if (str == null || str.length() == 0) {
            return null;
        }

        if (type == null) {
            return str;
        }

        ColumnType columnType = ColumnType.fromString(type.toUpperCase());
        Object ret;
        switch (columnType) {
            case TINYINT:
                ret = Byte.valueOf(str.trim());
                break;
            case SMALLINT:
                ret = Short.valueOf(str.trim());
                break;
            case INT:
                ret = Integer.valueOf(str.trim());
                break;
            case BIGINT:
                ret = Long.valueOf(str.trim());
                break;
            case FLOAT:
                ret = Float.valueOf(str.trim());
                break;
            case DOUBLE:
            case DECIMAL:
                ret = Double.valueOf(str.trim());
                break;
            case STRING:
            case VARCHAR:
            case CHAR:
                if (customDateFormat != null) {
                    ret = DateUtils.columnToDate(str, customDateFormat);
                    ret = DateUtils.timestampToString((Date) ret);
                } else {
                    ret = str;
                }
                break;
            case BOOLEAN:
                ret = Boolean.valueOf(str.trim().toLowerCase());
                break;
            case DATE:
                ret = DateUtils.columnToDate(str, customDateFormat);
                break;
            case TIMESTAMP:
                ret = DateUtils.columnToTimestamp(str, customDateFormat);
                break;
            default:
                throw new IllegalArgumentException("Unsupported field type:" + type);
        }

        return ret;
    }

    public static Object getWritableValue(Object writable) {
        Class<?> clz = writable.getClass();
        Object ret;

        if (clz == IntWritable.class) {
            ret = ((IntWritable) writable).get();
        } else if (clz == Text.class) {
            ret = writable.toString();
        } else if (clz == LongWritable.class) {
            ret = ((LongWritable) writable).get();
        } else if (clz == ByteWritable.class) {
            ret = ((ByteWritable) writable).get();
        } else if (clz == DateWritable.class) {
            ret = ((DateWritable) writable).get();
        } else if (writable instanceof DoubleWritable) {
            ret = ((DoubleWritable) writable).get();
        } else {
            ret = writable.toString();
        }

        return ret;
    }

    public static ObjectInspector columnTypeToObjectInspetor(ColumnType columnType) {
        ObjectInspector objectInspector;
        switch (columnType) {
            case TINYINT:
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Byte.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case SMALLINT:
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Short.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case INT:
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Integer.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case BIGINT:
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case FLOAT:
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Float.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case DOUBLE:
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Double.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case DECIMAL:
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(HiveDecimalWritable.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case TIMESTAMP:
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(java.sql.Timestamp.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case DATE:
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(java.sql.Date.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case STRING:
            case VARCHAR:
            case CHAR:
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case BOOLEAN:
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(Boolean.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            case BINARY:
                objectInspector = ObjectInspectorFactory.getReflectionObjectInspector(BytesWritable.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                break;
            default:
                throw new IllegalArgumentException("You should not be here");
        }
        return objectInspector;
    }


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


}