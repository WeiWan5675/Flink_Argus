package org.weiwan.argus.channel;

import org.weiwan.argus.core.pub.config.ArgusContext;
import org.weiwan.argus.core.pub.streaming.ArgusChannelHandler;
import org.weiwan.argus.core.pub.pojo.DataField;
import org.weiwan.argus.core.pub.pojo.DataRecord;
import org.weiwan.argus.core.pub.pojo.DataRow;

/**
 * @Author: xiaozhennan
 * @Date: 2020/8/12 16:58
 * @Package: org.weiwan.argus.channel.ExampleChannelHandler
 * @ClassName: ExampleChannelHandler
 * @Description:
 **/
public class ExampleChannelHandler extends ArgusChannelHandler<DataRecord<DataRow<DataField>>, DataRecord<DataRow<DataField>>> {

    private String channelVar;

    public ExampleChannelHandler(ArgusContext argusContext) {
        super(argusContext);
        this.channelVar = argusContext.getJobConfig().getChannelConfig().getStringVal("channel.example.channelVar");
    }


    @Override
    public DataRecord<DataRow<DataField>> process(DataRecord<DataRow<DataField>> value) {
        System.out.println("ExampleChannelHandler处理数据:" + value.toString());
        return value;
    }
}
