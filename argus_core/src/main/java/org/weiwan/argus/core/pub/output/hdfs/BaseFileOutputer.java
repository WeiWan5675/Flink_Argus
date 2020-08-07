package org.weiwan.argus.core.pub.output.hdfs;

import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.weiwan.argus.core.pub.pojo.DataField;
import org.weiwan.argus.core.pub.pojo.DataRecord;

import java.util.*;

/**
 * @Author: xiaozhennan
 * @Date: 2020/8/7 10:39
 * @Package: org.weiwan.argus.core.pub.output.hdfs
 * @ClassName: BaseFileOutputer
 * @Description:
 **/
public abstract class BaseFileOutputer<T extends Row> implements FileOutputer<T> {


    /**
     * 如果是字段名称匹配,需要制定字段名称
     * 如果是列匹配,只需要按照传递进来的顺序就可以
     */

    protected LinkedHashMap<String, DataField> columnTypes;

    //匹配模式,如果是mapping匹配模式时,必须要指定FieldType
    protected MatchMode matchMode = MatchMode.ALIGNMENT;
    protected String blockPath;
    protected Configuration configuration;
    protected boolean inited = false;
    protected FileOutputFormat fileOutputFormat;
    protected FileSystem fileSystem;

    public BaseFileOutputer(Configuration configuration, String path) {
        this.configuration = configuration;
        this.blockPath = path;
    }

    @Override
    public void init(List<DataField> fields, MatchMode matchMode) {
        this.matchMode = matchMode;
        if (MatchMode.MAPPING == matchMode && (fields == null || fields.size() < 1)) {
            //是匹配模式,但是没有给定fields,直接抛出异常
            throw new RuntimeException(String.format("select MatchMode: %s but fields is empty", matchMode.name()));
        }
        if (columnTypes == null && fields != null && fields.size() > 0) {
            columnTypes = new LinkedHashMap<>(fields.size());
        } else {
            return;
        }
        //初始化数据类型
        for (DataField field : fields) {
            columnTypes.put(field.getFieldKey(), field);
        }
        initOutput();
        inited = true;
    }


    @Override
    public boolean output(DataRecord<T> data) {

        //没有初始化,并且是index匹配模式
        if (!inited && MatchMode.ALIGNMENT == matchMode) {
            //使用数据的类型进行初始化
            Row firstRow = data.getData();
            columnTypes = new LinkedHashMap<>();
            for (int i = 0; i < firstRow.getArity(); i++) {
                DataField field = (DataField) firstRow.getField(i);
                String fieldKey = field.getFieldKey();
                columnTypes.put(fieldKey, field);
            }
            initOutput();
            inited = true;
        }

        //数据中字段类型需要匹配
        Row row = data.getData();
        Map<String, DataField> dataFieldMap = transformRow2Map(row);
        List<Object> datas = new ArrayList<Object>();
        switch (matchMode) {
            case MAPPING:
                for (String key : columnTypes.keySet()) {
                    DataField dataField = dataFieldMap.get(key);
                    Object isOK = converTypeInternal(dataField.getValue(), columnTypes.get(key).getFieldType());
                    datas.add(isOK);
                }
                break;
            case ALIGNMENT:
                //直接写出

            default:
                System.out.println("没有匹配的映射模式");
        }


        return false;
    }

    private Map<String, DataField> transformRow2Map(Row row) {
        Map<String, DataField> res = new HashMap();
        for (int i = 0; i < row.getArity(); i++) {
            DataField field = (DataField) row.getField(i);
            if (field != null) {
                res.put(field.getFieldKey(), field);
            }
        }
        return res;
    }


    protected abstract void initOutput();

    public abstract boolean out(List<Object> data);

    public abstract Object converType(Object obj);

    //提供一套默认转换
    public Object converTypeInternal(Object obj, ColumnType columnType) {
        Object o = converType(obj);
        return o;
    }


}
