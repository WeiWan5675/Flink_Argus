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

    void init(List<DataField> fields) throws IOException;

    void output(DataRecord<T> data) throws Exception;

    void batchOutput(List<DataRecord<T>> dataRecords) throws Exception;

    void close() throws IOException;


}
