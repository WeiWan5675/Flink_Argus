package org.weiwan.argus.channel;

import org.weiwan.argus.core.pub.config.ArgusContext;
import org.weiwan.argus.core.pub.streaming.ArgusChannelHandler;
import org.weiwan.argus.core.pub.pojo.DataRecord;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/20 16:49
 * @Package: org.weiwan.argus.channel
 * @ClassName: CommonChannelHandler
 * @Description:
 **/
public class CommonChannelHandler extends ArgusChannelHandler<DataRecord, DataRecord> {
    public CommonChannelHandler(ArgusContext context) {
        super(context);
    }

    @Override
    public DataRecord process(DataRecord value) {
//        System.out.println("channel处理数据!!!!" + value.getData().toString());
        return new DataRecord(value.getData());
    }
}
