package org.weiwan.argus.core.pub.api;

import org.weiwan.argus.core.pub.api.ArgusOutputFormatSink;
import org.weiwan.argus.core.pub.pojo.DataRecord;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/19 20:12
 * @Package: org.weiwan.argus.core
 * @ClassName: ArgusWriter
 * @Description:
 **/
public interface ArgusWriter<T> {
    ArgusOutputFormatSink<DataRecord<?>> writer();
}
