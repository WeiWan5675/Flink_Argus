package org.weiwan.argus.core.pub.api;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.streaming.api.datastream.DataStream;
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


    public BaseReader(StreamExecutionEnvironment env, ArgusContext argusContext) {
        this.env = env;
        this.argusContext = argusContext;
        this.jobConfig = argusContext.getJobConfig();
        this.readerConfig = argusContext.getJobConfig().getReaderConfig();
    }

    public abstract BaseRichInputFormat getInputFormat(ArgusContext context);


    @Override
    public DataStream<T> reader() {
        BaseRichInputFormat<T, InputSplit> inputFormat = getInputFormat(argusContext);
        TypeInformation<T> inputFormatTypes = TypeExtractor.getInputFormatTypes(inputFormat);
        ArgusInputFormatSource<T> tArgusInputFormatSource = new ArgusInputFormatSource<>(inputFormat,inputFormatTypes);
        DataStream<T> stream = env.addSource(tArgusInputFormatSource, "", inputFormatTypes);
        DataStream<T> afterStream = afterReading(stream, argusContext);
        return afterStream;
    }

    /**
     * <p> 方便某些自定义reader进行一些后处理工作
     *
     * @param stream {@link DataStream<DataRecord>} 输入是这个
     * @return 输出也是这个
     */
    protected DataStream<T> afterReading(DataStream<T> stream, ArgusContext context) {
        //do nothing
        return stream;
    }

    ;
}
