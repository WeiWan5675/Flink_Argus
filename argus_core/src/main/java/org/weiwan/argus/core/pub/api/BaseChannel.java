package org.weiwan.argus.core.pub.api;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.weiwan.argus.core.pub.config.ArgusContext;
import org.weiwan.argus.core.pub.handler.ArgusChannelHandler;
import org.weiwan.argus.core.pub.pojo.DataRecord;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/19 20:13
 * @Package: org.weiwan.argus.core
 * @ClassName: BaseChannel
 * @Description:
 **/
public abstract class BaseChannel<IN extends DataRecord, OUT extends DataRecord> implements ArgusChannel<IN, OUT> {
    private StreamExecutionEnvironment env;
    private ArgusContext argusContext;

    public BaseChannel(StreamExecutionEnvironment env, ArgusContext argusContext) {
        this.env = env;
        this.argusContext = argusContext;
    }

    public abstract ArgusChannelHandler getChannelHandler();

    @Override
    public DataStream<OUT> channel(DataStream<IN> dataStream) {
        ArgusChannelHandler channelHandler = getChannelHandler();
        DataStream<OUT> stream = dataStream.map(channelHandler);
        return stream;
    }
}
