package org.weiwan.argus.core.pub.api;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.weiwan.argus.core.pub.pojo.DataRecord;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/19 20:13
 * @Package: org.weiwan.argus.core
 * @ClassName: ArgusChannel
 * @Description:
 **/
public interface ArgusChannel<IN extends DataRecord,OUT extends DataRecord> {
    DataStream<OUT> channel(DataStream<IN> dataStream);
}
