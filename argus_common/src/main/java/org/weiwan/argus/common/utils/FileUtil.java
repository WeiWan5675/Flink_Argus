package org.weiwan.argus.common.utils;

import org.apache.commons.codec.Charsets;
import org.weiwan.argus.common.exception.ArgusCommonException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * @Author: xiaozhennan
 * @Date: 2020/8/12 17:46
 * @Package: org.weiwan.argus.common.utils.FileUtil
 * @ClassName: FileUtil
 * @Description:
 **/
public class FileUtil {

    public static String readFileContent(String path) throws IOException {
        //读取配置文件  转化成json对象
        File file = new File(path);
        if (!file.exists() || file.isDirectory()) {
            throw new ArgusCommonException("The configuration file does not exist, please check the configuration file path!");
        }
        FileInputStream in = new FileInputStream(file);
        byte[] filecontent = new byte[(int) file.length()];
        in.read(filecontent);
        return new String(filecontent, Charsets.UTF_8.name());
    }

}
