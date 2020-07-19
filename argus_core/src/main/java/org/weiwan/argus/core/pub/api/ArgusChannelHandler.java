package org.weiwan.argus.core.pub.api;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/19 20:36
 * @Package: org.weiwan.argus.core.pub.api
 * @ClassName: ArgusChannelHandler
 * @Description:
 **/
public abstract class ArgusChannelHandler<T, O> extends RichMapFunction<T, O> implements CheckpointedFunction {




}
