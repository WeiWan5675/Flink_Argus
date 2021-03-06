package org.weiwan.argus.core.pub.enums;

import java.io.Serializable;

/**
 * @Author: xiaozhennan
 * @Date: 2020/8/3 11:21
 * @Package: org.weiwan.argus.writer.hive
 * @ClassName: WriteMode
 * @Description:
 **/
public enum WriteMode implements Serializable {

    OVERWRITE("OVERWRITE", "覆盖写入"),
    APPEND("APPEND", "追加写入");

    private String code;
    private String msg;

    WriteMode(String code, String msg) {
        this.code = code;
        this.msg = msg;
    }


}
