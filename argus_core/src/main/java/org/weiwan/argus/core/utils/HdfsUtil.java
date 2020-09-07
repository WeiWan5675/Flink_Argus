package org.weiwan.argus.core.utils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * @Author: xiaozhennan
 * @Date: 2020/9/4 10:59
 * @Package: org.weiwan.argus.core.utils.HdfsUtil
 * @ClassName: HdfsUtil
 * @Description:
 **/
public class HdfsUtil {


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
