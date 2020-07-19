package org.weiwan.argus.core.pub.config;

import java.util.Map;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/17 16:38
 * @Package: org.weiwan.argus.core.pub.config
 * @ClassName: JobConfig
 * @Description:
 **/
public class JobConfig extends AbstractConfig {

    public static final String reader = "reader";

    public static final String writer = "writer";


    private ReaderConfig readerConfig;

    private WriterConfig writerConfig;

    private ChannelConfig channelConfig;

    public JobConfig(Map<String, Object> map) {
        super(map);
    }


    public ReaderConfig getReaderConfig() {
        return readerConfig;
    }

    public void setReaderConfig(ReaderConfig readerConfig) {
        this.readerConfig = readerConfig;
    }

    public WriterConfig getWriterConfig() {
        return writerConfig;
    }

    public void setWriterConfig(WriterConfig writerConfig) {
        this.writerConfig = writerConfig;
    }


    public ChannelConfig getChannelConfig() {
        return channelConfig;
    }

    public void setChannelConfig(ChannelConfig channelConfig) {
        this.channelConfig = channelConfig;
    }
}
