package org.weiwan.argus.core.pub.api;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.weiwan.argus.core.pub.api.ArgusOutputFormatSink;
import org.weiwan.argus.core.pub.pojo.DataRecord;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/19 20:12
 * @Package: org.weiwan.argus.core
 * @ClassName: ArgusWriter
 * @Description:
 **/
public interface ArgusWriter<T extends DataRecord> {
    DataStreamSink<T> writer(DataStream<T> dataStream);
}
