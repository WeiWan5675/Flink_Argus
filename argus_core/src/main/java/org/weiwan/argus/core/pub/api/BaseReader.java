package org.weiwan.argus.core.pub.api;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.io.InputSplit;
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

    public abstract BaseRichInputFormat<T, InputSplit> getInputFormat();


    @Override
    public ArgusInputFormatSource<T> reader() {
        BaseRichInputFormat<T, InputSplit> inputFormat = getInputFormat();
        TypeInformation<T> inputFormatTypes = TypeExtractor.getInputFormatTypes(inputFormat);
        ArgusInputFormatSource<T> tArgusInputFormatSource = new ArgusInputFormatSource<>(inputFormat, inputFormatTypes);

        return tArgusInputFormatSource;
    }
}
