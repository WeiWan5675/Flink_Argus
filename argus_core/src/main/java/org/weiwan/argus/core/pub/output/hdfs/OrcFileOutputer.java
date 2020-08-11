package org.weiwan.argus.core.pub.output.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.weiwan.argus.core.pub.pojo.DataField;
import org.weiwan.argus.core.pub.pojo.DataRecord;
import org.weiwan.argus.core.pub.pojo.DataRow;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @Author: xiaozhennan
 * @Date: 2020/8/5 17:54
 * @Package: org.weiwan.argus.core.pub.output.hdfs
 * @ClassName: OrcFileOutputer
 * @Description:
 **/
public class OrcFileOutputer extends BaseFileOutputer<DataRow> {

    public OrcFileOutputer(Configuration configuration, FileSystem fileSystem) {
        super(configuration, fileSystem);
    }

    @Override
    public void closeOutputer() {

    }

    @Override
    public void initOutputer() throws IOException {

    }

    @Override
    public boolean out(Map<String, DataField> data) throws Exception {
        return false;
    }

    @Override
    public boolean batchOutput(List<DataRecord<DataRow>> dataRecords) {
        return false;
    }

    @Override
    public void writeNextBlock(String nextFileBlock) {

    }
}
