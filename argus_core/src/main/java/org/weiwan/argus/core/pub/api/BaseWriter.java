package org.weiwan.argus.core.pub.api;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.weiwan.argus.core.pub.config.ArgusContext;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/14 17:03
 * @Package: org.weiwan.argus.pub.api
 * @ClassName: BaseWriter
 * @Description:
 **/
public abstract class BaseWriter implements ArgusWriter {



    private StreamExecutionEnvironment env;

    private ArgusContext argusContext;

}
