package org.weiwan.argus.core.pub.output.hdfs;

import org.apache.flink.types.Row;
import org.weiwan.argus.core.pub.pojo.DataField;
import org.weiwan.argus.core.pub.pojo.DataRecord;

import java.util.List;
import java.util.Map;

/**
 * @Author: xiaozhennan
 * @Date: 2020/8/5 17:52
 * @Package: org.weiwan.argus.core.pub.output.hdfs
 * @ClassName: FileOutputer
 * @Description:
 **/
public interface FileOutputer<T extends Row> {

    public void init(List<DataField> fields, MatchMode matchMode);

    public boolean output(DataRecord<T> data);

}
