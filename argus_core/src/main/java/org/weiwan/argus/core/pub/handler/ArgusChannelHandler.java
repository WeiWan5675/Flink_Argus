package org.weiwan.argus.core.pub.handler;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.weiwan.argus.core.pub.config.ArgusContext;
import org.weiwan.argus.core.pub.pojo.DataRecord;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/19 20:36
 * @Package: org.weiwan.argus.core.pub.api
 * @ClassName: ArgusChannelHandler
 * @Description:
 **/
public abstract class ArgusChannelHandler<T extends DataRecord, O extends DataRecord> extends RichMapFunction<T, O> implements CheckpointedFunction {


    @Override
    public O map(T value) throws Exception {
        return process(value);
    }


    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

    }


    public abstract O process(T value);
}
