package org.weiwan.argus.core.pub.api;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.weiwan.argus.core.pub.config.ArgusContext;
import org.weiwan.argus.core.pub.config.JobConfig;
import org.weiwan.argus.core.pub.config.WriterConfig;
import org.weiwan.argus.core.pub.output.BaseRichOutputFormat;
import org.weiwan.argus.core.pub.pojo.DataRecord;
import org.weiwan.argus.core.pub.streaming.ArgusOutputFormatSink;

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
    protected JobConfig jobConfig;
    protected WriterConfig writerConfig;
    protected String writerName;
    protected String writerType;
    protected String writerClassName;
    protected Integer writerParallelism;

    private static final String KEY_WRITER_NAME = "writer.name";
    private static final String KEY_WRITER_TYPE = "writer.type";
    private static final String KEY_WRITER_CLASS_NAME = "writer.class";
    private static final String KEY_WRITER_PARALLELISM = "writer.parallelism";


    public BaseWriter(StreamExecutionEnvironment env, ArgusContext argusContext) {
        this.env = env;
        this.argusContext = argusContext;
        this.jobConfig = argusContext.getJobConfig();
        this.writerConfig = argusContext.getJobConfig().getWriterConfig();
        this.writerName = writerConfig.getWriterName();
        this.writerClassName = writerConfig.getStringVal(KEY_WRITER_CLASS_NAME);
        this.writerType = writerConfig.getStringVal(KEY_WRITER_TYPE);
        this.writerParallelism = writerConfig.getIntVal(KEY_WRITER_PARALLELISM, 1);
    }

    public abstract BaseRichOutputFormat<T> getOutputFormat();

    @Override
    public DataStreamSink<T> writer(DataStream<T> dataStream) {
        DataStream<T> beforeWritingStream = beforeWriting(dataStream);
        BaseRichOutputFormat<T> outputFormat = getOutputFormat();
        ArgusOutputFormatSink<T> outputFormatSink = new ArgusOutputFormatSink<T>(outputFormat);
        DataStreamSink<T> sink = beforeWritingStream.addSink(outputFormatSink);
        sink.setParallelism(writerParallelism);
        return sink;
    }

    protected DataStream<T> beforeWriting(DataStream<T> dataStream) {
        return dataStream;
    }
}
