package org.weiwan.argus.core.pub.output.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.weiwan.argus.core.pub.enums.CompressType;
import org.weiwan.argus.core.pub.enums.FileType;

/**
 * @Author: xiaozhennan
 * @Date: 2020/8/5 17:23
 * @Package: org.weiwan.argus.core.pub.output.hdfs
 * @ClassName: FileOutputFacory
 * @Description:
 **/
public class FileOutputFacory {

    //拿到数据才能生成对应的

    public FileOutputFormat generateOutputFormat(FileType fileType, CompressType compressType, Configuration configuration) {
        FileOutputFormat outputFormat = null;
        switch (fileType) {
            case TEXT:
                outputFormat = generateOutputFormatForText(fileType, compressType, configuration);
                break;
            case ORC:
                outputFormat = generateOutputFormatForOrc(fileType, compressType, configuration);
                break;
            case PARQUET:
                outputFormat = generateOutputFormatForParquet(fileType, compressType, configuration);
                break;
            default:
                return outputFormat;
        }
        return outputFormat;
    }

    private FileOutputFormat generateOutputFormatForParquet(FileType fileType, CompressType compressType, Configuration configuration) {
        return null;
    }

    private FileOutputFormat generateOutputFormatForOrc(FileType fileType, CompressType compressType, Configuration configuration) {

        return null;
    }

    private FileOutputFormat generateOutputFormatForText(FileType fileType, CompressType compressType, Configuration configuration) {

        return null;
    }
}
