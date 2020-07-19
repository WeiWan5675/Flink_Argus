package org.weiwan.argus.core.pub.api;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.weiwan.argus.core.pub.config.ArgusContext;
import org.weiwan.argus.core.pub.pojo.DataRecord;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/19 20:13
 * @Package: org.weiwan.argus.core
 * @ClassName: BaseChannel
 * @Description:
 **/
public class BaseChannel<IN extends DataRecord, OUT extends DataRecord> implements ArgusChannel<IN, OUT> {

    private StreamExecutionEnvironment env;
    private ArgusContext argusContext;

    @Override
    public ArgusChannelHandler<IN, OUT> channel() {
        return null;
    }
}
