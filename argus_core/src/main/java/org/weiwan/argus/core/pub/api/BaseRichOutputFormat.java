package org.weiwan.argus.core.pub.api;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.weiwan.argus.core.pub.config.ArgusContext;
import org.weiwan.argus.core.pub.config.JobConfig;
import org.weiwan.argus.core.pub.config.WriterConfig;
import org.weiwan.argus.core.pub.pojo.DataRecord;

import java.io.IOException;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/14 17:23
 * @Package: org.weiwan.argus.pub.api
 * @ClassName: BaseRichOutputFormat
 * @Description:
 **/
public abstract class BaseRichOutputFormat<T extends DataRecord> extends RichOutputFormat<T> {

    protected ArgusContext argusContext;
    protected JobFormatState formatState;
    protected WriterConfig writerConfig;
    protected JobConfig jobConfig;



    protected int i = 0;
    public BaseRichOutputFormat(ArgusContext argusContext) {
        this.argusContext = argusContext;
    }


    @Override
    public void configure(Configuration parameters) {

    }


    @Override
    public void open(int taskNumber, int numTasks) throws IOException {

    }


    @Override
    public void writeRecord(T record) throws IOException {
        i++;
        System.out.println("hiveWriter处理数据:" + record);
    }


    @Override
    public void close() throws IOException {
        System.out.println(i);
    }

}
