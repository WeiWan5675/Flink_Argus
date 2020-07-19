package org.weiwan.argus.core.pub.api;

import org.weiwan.argus.core.pub.api.ArgusInputFormatSource;
import org.weiwan.argus.core.pub.pojo.DataRecord;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/19 20:11
 * @Package: org.weiwan.argus.core
 * @ClassName: ArgusReader
 * @Description:
 **/
public interface ArgusReader<T extends DataRecord> {
    ArgusInputFormatSource<T> reader();
}
