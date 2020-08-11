package org.weiwan.argus.core.pub.output.hdfs;

import org.weiwan.argus.core.pub.enums.CompressType;
import org.weiwan.argus.core.pub.enums.FileType;
import org.weiwan.argus.core.pub.pojo.DataField;
import org.weiwan.argus.core.pub.pojo.DataRecord;
import org.weiwan.argus.core.pub.pojo.DataRow;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @Author: xiaozhennan
 * @Date: 2020/8/5 17:52
 * @Package: org.weiwan.argus.core.pub.output.hdfs
 * @ClassName: FileOutputer
 * @Description:
 **/
public interface FileOutputer<T extends DataRow> {

    public void init(List<DataField> fields) throws IOException;

    public boolean output(DataRecord<T> data) throws Exception;

    public boolean batchOutput(List<DataRecord<T>> dataRecords) throws Exception;

    public void close();

    /**
     * 返回当前文件块大小(Bytes) 默认 1024 * 1024 * 1024 64M
     * @return
     */
    Long getCurrentFileBlockSize();

    void writeNextBlock(String nextFileBlock);

}
