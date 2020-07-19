package org.weiwan.argus.core.pub.api;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.weiwan.argus.core.pub.config.ArgusContext;
import org.weiwan.argus.core.pub.pojo.DataRecord;


/**
 * @Author: xiaozhennan
 * @Date: 2020/7/14 17:02
 * @Package: org.weiwan.argus.pub.api
 * @ClassName: ReaderBase
 * @Description:
 **/
public abstract class BaseReader<T extends DataRecord> implements ArgusReader {


    private StreamExecutionEnvironment env;

    private ArgusContext argusContext;






}
