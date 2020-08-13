package org.weiwan.argus.channel;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.weiwan.argus.core.pub.api.BaseChannel;
import org.weiwan.argus.core.pub.config.ArgusContext;
import org.weiwan.argus.core.pub.handler.ArgusChannelHandler;
import org.weiwan.argus.core.pub.pojo.DataField;
import org.weiwan.argus.core.pub.pojo.DataRecord;
import org.weiwan.argus.core.pub.pojo.DataRow;

/**
 * @Author: xiaozhennan
 * @Date: 2020/8/12 16:56
 * @Package: org.weiwan.argus.channel.ExampleChannel
 * @ClassName: ExampleChannel
 * @Description:
 **/
public class ExampleChannel extends BaseChannel<DataRecord<DataRow<DataField>>, DataRecord<DataRow<DataField>>> {
    public static final Logger logger = LoggerFactory.getLogger(ExampleChannel.class);
    public ExampleChannel(StreamExecutionEnvironment env, ArgusContext argusContext) {
        super(env, argusContext);
    }

    @Override
    public ArgusChannelHandler<DataRecord<DataRow<DataField>>, DataRecord<DataRow<DataField>>> getChannelHandler(ArgusContext argusContext) {
        logger.info("this Method Returns ExampleChannel");
        return new ExampleChannelHandler(argusContext);
    }
}
