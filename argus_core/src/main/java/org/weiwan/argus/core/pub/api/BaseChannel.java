package org.weiwan.argus.core.pub.api;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.weiwan.argus.core.pub.config.ArgusContext;
import org.weiwan.argus.core.pub.config.ChannelConfig;
import org.weiwan.argus.core.pub.config.JobConfig;
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
    protected final JobConfig jobConfig;
    protected StreamExecutionEnvironment env;
    protected String channelName;
    protected ArgusContext argusContext;
    protected ChannelConfig channelConfig;

    public BaseChannel(StreamExecutionEnvironment env, ArgusContext argusContext) {
        this.env = env;
        this.argusContext = argusContext;
        this.jobConfig = argusContext.getJobConfig();
        this.channelConfig = argusContext.getJobConfig().getChannelConfig();
        this.channelName = channelConfig.getChannleName();
    }

    public abstract ArgusChannelHandler getChannelHandler();

    @Override
    public DataStream<OUT> channel(DataStream<IN> dataStream) {
        ArgusChannelHandler channelHandler = getChannelHandler();
        DataStream<OUT> stream = dataStream.map(channelHandler);
        return stream;
    }
}
