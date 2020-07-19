package org.weiwan.argus.core.pub.api;

import org.weiwan.argus.core.pub.api.ArgusChannelHandler;
import org.weiwan.argus.core.pub.pojo.DataRecord;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/19 20:13
 * @Package: org.weiwan.argus.core
 * @ClassName: ArgusChannel
 * @Description:
 **/
public interface ArgusChannel<IN extends DataRecord,OUT extends DataRecord> {
    ArgusChannelHandler<IN,OUT> channel();
}
