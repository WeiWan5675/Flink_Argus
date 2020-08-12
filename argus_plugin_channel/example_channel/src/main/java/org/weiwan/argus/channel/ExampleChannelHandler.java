package org.weiwan.argus.channel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.weiwan.argus.core.pub.config.ArgusContext;
import org.weiwan.argus.core.pub.handler.ArgusChannelHandler;
import org.weiwan.argus.core.pub.pojo.DataRecord;

/**
 * @Author: xiaozhennan
 * @Date: 2020/8/12 16:58
 * @Package: org.weiwan.argus.channel.ExampleChannelHandler
 * @ClassName: ExampleChannelHandler
 * @Description:
 **/
public class ExampleChannelHandler extends ArgusChannelHandler {

    private String channelVar;

    public ExampleChannelHandler(ArgusContext context) {
        super(context);
        this.channelVar = channelConfig.getStringVal("channel.example.channelVar");
    }

    @Override
    public DataRecord process(DataRecord value) {
        System.out.println("ExampleChannelHandler处理数据:" + value.toString());
        return value;
    }

}
