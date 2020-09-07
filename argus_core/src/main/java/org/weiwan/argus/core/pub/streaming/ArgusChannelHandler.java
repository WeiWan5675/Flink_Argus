package org.weiwan.argus.core.pub.streaming;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.mapred.JobConf;
import org.weiwan.argus.core.pub.config.ArgusContext;
import org.weiwan.argus.core.pub.config.ChannelConfig;
import org.weiwan.argus.core.pub.config.JobConfig;
import org.weiwan.argus.core.pub.pojo.DataRecord;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/19 20:36
 * @Package: org.weiwan.argus.core.pub.api
 * @ClassName: ArgusChannelHandler
 * @Description:
 **/
public abstract class ArgusChannelHandler<T extends DataRecord, O extends DataRecord> extends RichMapFunction<T, O> implements CheckpointedFunction {

    protected ArgusContext context;
    protected JobConfig jobConfig;
    protected ChannelConfig channelConfig;

    public ArgusChannelHandler(ArgusContext context) {
        this.context = context;
        this.jobConfig = context.getJobConfig();
        this.channelConfig = context.getJobConfig().getChannelConfig();
    }

    public ArgusChannelHandler() {
    }

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
