package org.weiwan.argus.core.pub.enums;

import java.io.Serializable;

/**
 * @Author: xiaozhennan
 * @Date: 2020/8/3 11:24
 * @Package: org.weiwan.argus.writer.hive
 * @ClassName: CompressType
 * @Description:
 **/
public enum CompressType implements Serializable {
    GZIP("GZIP", "GIZP"),
    BIZP2("BIZP2", "BIZP2"),
    SNAPPY("SNAPPY", "SNAPPY"),
    LZ4("LZ4", "LZ4"),
    NONE("NONE", "none");

    private String code;
    private String msg;

    CompressType(String code, String msg) {
        this.code = code;
        this.msg = msg;
    }
}
