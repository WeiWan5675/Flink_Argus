package org.weiwan.argus.core.pub.output.hdfs;

import java.io.Serializable;

/**
 * @Author: xiaozhennan
 * @Date: 2020/8/3 11:28
 * @Package: org.weiwan.argus.writer.hive
 * @ClassName: MatchMode
 * @Description:
 **/
public enum MatchMode implements Serializable {
    MAPPING("MAPPING", "Mapping"),
    ALIGNMENT("ALIGNMENT", "Alignment");

    private String code;

    private String msg;

    MatchMode(String code, String msg) {
        this.code = code;
        this.msg = msg;
    }
}
