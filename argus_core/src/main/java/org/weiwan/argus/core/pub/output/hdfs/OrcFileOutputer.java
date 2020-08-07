package org.weiwan.argus.core.pub.output.hdfs;

import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.weiwan.argus.core.pub.pojo.DataRecord;

import java.util.List;
import java.util.Map;

/**
 * @Author: xiaozhennan
 * @Date: 2020/8/5 17:54
 * @Package: org.weiwan.argus.core.pub.output.hdfs
 * @ClassName: OrcFileOutputer
 * @Description:
 **/
public class OrcFileOutputer extends BaseFileOutputer<Row> {


    public OrcFileOutputer(Configuration configuration, String path) {
        super(configuration, path);
    }

    @Override
    protected void initOutput() {

    }

    @Override
    public boolean out(List<Object> data) {
        return false;
    }

    @Override
    public Object converType(Object obj) {
        return null;
    }
}
