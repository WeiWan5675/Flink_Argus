package org.weiwan.argus.channel;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.weiwan.argus.core.pub.handler.ArgusChannelHandler;
import org.weiwan.argus.core.pub.api.BaseChannel;
import org.weiwan.argus.core.pub.config.ArgusContext;
import org.weiwan.argus.core.pub.pojo.DataRecord;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/20 14:01
 * @Package: org.weiwan.argus.channel
 * @ClassName: CommonChannel
 * @Description:
 **/
public class CommonChannel extends BaseChannel<DataRecord, DataRecord> {

    public CommonChannel(StreamExecutionEnvironment env, ArgusContext argusContext) {
        super(env, argusContext);
    }

    @Override
    public ArgusChannelHandler getChannelHandler() {
        CommonChannelHandler handler = new CommonChannelHandler(argusContext);
        return handler;
    }
}
