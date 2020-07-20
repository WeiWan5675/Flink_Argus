package org.weiwan.argus.core.pub.api;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.weiwan.argus.core.pub.config.ArgusContext;
import org.weiwan.argus.core.pub.pojo.DataRecord;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/14 17:03
 * @Package: org.weiwan.argus.pub.api
 * @ClassName: BaseWriter
 * @Description:
 **/
public abstract class BaseWriter<T extends DataRecord> implements ArgusWriter<T> {

    protected StreamExecutionEnvironment env;

    protected ArgusContext argusContext;

    public BaseWriter(StreamExecutionEnvironment env, ArgusContext argusContext) {
        this.env = env;
        this.argusContext = argusContext;
    }

    public abstract BaseRichOutputFormat getOutputFormat();

    public DataStream<T> beforeWriting(DataStream<T> dataStream) {
        return dataStream;
    }

    @Override
    public DataStreamSink<T> writer(DataStream<T> dataStream) {
        DataStream<T> beforeWritingStream = beforeWriting(dataStream);
        BaseRichOutputFormat outputFormat = getOutputFormat();
        ArgusOutputFormatSink<T> outputFormatSink = new ArgusOutputFormatSink<T>(outputFormat);
        DataStreamSink<T> sink = beforeWritingStream.addSink(outputFormatSink);
        return sink;
    }
}
