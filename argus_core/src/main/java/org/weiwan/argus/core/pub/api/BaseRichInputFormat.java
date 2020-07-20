package org.weiwan.argus.core.pub.api;

import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.types.Row;
import org.weiwan.argus.core.pub.pojo.DataRecord;

import java.io.IOException;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/14 17:23
 * @Package: org.weiwan.argus.pub.api
 * @ClassName: BaseInputFormat
 * @Description:
 **/
public abstract class BaseRichInputFormat<OT, T extends InputSplit> extends RichInputFormat<OT, T> {


    @Override
    public void configure(Configuration parameters) {

    }


    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
        return null;
    }

    @Override
    public T[] createInputSplits(int minNumSplits) throws IOException {
        return null;
    }


    @Override
    public InputSplitAssigner getInputSplitAssigner(T[] inputSplits) {
        return null;
    }


    @Override
    public void open(T split) throws IOException {

    }


    @Override
    public boolean reachedEnd() throws IOException {
        return false;
    }


    @Override
    public OT nextRecord(OT reuse) throws IOException {

        return null;
    }


    @Override
    public void close() throws IOException {

    }
}
