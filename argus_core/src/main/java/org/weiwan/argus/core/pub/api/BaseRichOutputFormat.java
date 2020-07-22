package org.weiwan.argus.core.pub.api;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.weiwan.argus.core.pub.config.ArgusContext;

import java.io.IOException;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/14 17:23
 * @Package: org.weiwan.argus.pub.api
 * @ClassName: BaseRichOutputFormat
 * @Description:
 **/
public abstract class BaseRichOutputFormat<IT> extends RichOutputFormat<IT> {

    protected ArgusContext argusContext;

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
    public void writeRecord(IT record) throws IOException {
        System.out.println("hiveWriter处理数据:" + record);
    }


    @Override
    public void close() throws IOException {

    }

}
