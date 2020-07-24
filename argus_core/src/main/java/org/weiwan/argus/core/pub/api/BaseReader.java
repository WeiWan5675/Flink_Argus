package org.weiwan.argus.core.pub.api;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.weiwan.argus.core.pub.config.ArgusContext;
import org.weiwan.argus.core.pub.config.JobConfig;
import org.weiwan.argus.core.pub.config.ReaderConfig;
import org.weiwan.argus.core.pub.pojo.DataRecord;


/**
 * @Author: xiaozhennan
 * @Date: 2020/7/14 17:02
 * @Package: org.weiwan.argus.pub.api
 * @ClassName: ReaderBase
 * @Description:
 **/
public abstract class BaseReader<T extends DataRecord> implements ArgusReader<T> {

    private StreamExecutionEnvironment env;
    private ArgusContext argusContext;
    private JobConfig jobConfig;
    private ReaderConfig readerConfig;

    private static final String KEY_READER_NAME = "reader.name";
    private static final String KEY_READER_TYPE = "reader.type";
    private static final String KEY_READER_CLASS_NAME = "reader.class";
    private static final String KEY_READER_PARALLELISM = "reader.parallelism";

    protected String readerName;
    protected String readerType;
    protected String readerClassName;
    protected Integer readerParallelism;

    public BaseReader(StreamExecutionEnvironment env, ArgusContext argusContext) {
        this.env = env;
        this.argusContext = argusContext;
        this.jobConfig = argusContext.getJobConfig();
        this.readerConfig = argusContext.getJobConfig().getReaderConfig();
        this.readerName = readerConfig.getStringVal(KEY_READER_NAME,"ArugsReader");
        this.readerType = readerConfig.getStringVal(KEY_READER_TYPE);
        this.readerClassName = readerConfig.getStringVal(KEY_READER_CLASS_NAME);
        this.readerParallelism = readerConfig.getIntVal(KEY_READER_PARALLELISM, 1);
    }

    public abstract BaseRichInputFormat getInputFormat(ArgusContext context);


    @Override
    public DataStream<T> reader() {
        BaseRichInputFormat<T, BaseInputSpliter> inputFormat = getInputFormat(argusContext);
        TypeInformation<T> inputFormatTypes = TypeExtractor.getInputFormatTypes(inputFormat);
        ArgusInputFormatSource<T> tArgusInputFormatSource = new ArgusInputFormatSource<>(inputFormat, inputFormatTypes);
        DataStreamSource<T> streamSource = env.addSource(tArgusInputFormatSource, readerName, inputFormatTypes);
        streamSource.setParallelism(readerParallelism);
        DataStream<T> afterStream = afterReading(streamSource, argusContext);
        return afterStream;
    }

    /**
     * <p> 方便某些自定义reader进行一些后处理工作
     *
     * @param stream 输入是 {@link DataStream<DataRecord>}
     * @return 输出也是 {@link DataStream<DataRecord>}
     */
    protected DataStream<T> afterReading(DataStream<T> stream, ArgusContext context) {
        //do nothing
        return stream;
    }

    ;
}
