package org.weiwan.argus.core.pub.output.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.weiwan.argus.core.pub.enums.ColumnType;
import org.weiwan.argus.core.pub.enums.CompressType;
import org.weiwan.argus.core.pub.enums.FileType;
import org.weiwan.argus.core.pub.pojo.DataField;
import org.weiwan.argus.core.pub.pojo.DataRecord;
import org.weiwan.argus.core.pub.pojo.DataRow;

import java.io.IOException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.*;

/**
 * @Author: xiaozhennan
 * @Date: 2020/8/7 10:39
 * @Package: org.weiwan.argus.core.pub.output.hdfs
 * @ClassName: BaseFileOutputer
 * @Description:
 **/
public abstract class BaseFileOutputer<T extends DataRow> implements FileOutputer<T> {


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
    protected CompressType compressType;
    protected FileType fileType;
    protected String charsetName = "UTF-8";
    protected String lineDelimiter = "\n";
    protected String fieldDelimiter = "\u0001";

    protected boolean isBatchWriteMode = true;
    protected int batchWriteSize = 1000;

    public BaseFileOutputer(Configuration configuration, FileSystem fileSystem) {
        this.configuration = configuration;
        this.fileSystem = fileSystem;
    }

    @Override
    public void init(List<DataField> fields) throws IOException {
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

        initOutputer();
        inited = true;
    }


    @Override
    public void output(DataRecord<T> data) throws Exception {
        //没有初始化,并且是index匹配模式
        if (!inited && MatchMode.ALIGNMENT == matchMode) {
            //使用数据的类型进行初始化
            DataRow firstRow = data.getData();
            columnTypes = new LinkedHashMap<>();
            for (int i = 0; i < firstRow.getArity(); i++) {
                DataField field = (DataField) firstRow.getField(i);
                String fieldKey = field.getFieldKey();
                columnTypes.put(fieldKey, field);
            }
            initOutputer();
            inited = true;
        }

        //数据中字段类型需要匹配
        DataRow row = data.getData();
        Map<String, DataField> dataFieldMap = transformRow2Map(row);
        this.out(dataFieldMap);
    }

    @Override
    public void batchOutput(List<DataRecord<T>> dataRecords) throws Exception {
        for (DataRecord<T> dataRecord : dataRecords) {
            output(dataRecord);
        }
    }

    private Map<String, DataField> transformRow2Map(DataRow row) {
        Map<String, DataField> res = new HashMap();
        for (int i = 0; i < row.getArity(); i++) {
            DataField field = (DataField) row.getField(i);
            if (field != null) {
                Object value = field.getValue();
                ColumnType fieldType = field.getFieldType();
                Object o = converTypeInternal(value, fieldType);
                field.setValue(o);
                res.put(field.getFieldKey(), field);
            }
        }
        return res;
    }



    @Override
    public void close() {
        closeOutputer();
    }

    public abstract void closeOutputer();

    public abstract void initOutputer() throws IOException;

    public abstract void out(Map<String, DataField> data) throws Exception;


    //提供一套默认转换
    public Object converTypeInternal(Object obj, ColumnType columnType) {

        switch (columnType) {
            case STRING:
            case VARCHAR:
            case VARCHAR2:
            case NVARCHAR:
            case TEXT:
            case BINARY:
            case JSON:
                return String.valueOf(obj);
            case TINYINT:
            case INTEGER:
            case INT:
            case INT32:
            case INT64:
                return Integer.valueOf(obj.toString());

            case LONG:
            case BIGINT:
                return Long.valueOf(obj.toString());


            case TIMESTAMP:
                return Timestamp.valueOf(obj.toString());
            case TIME:
                return Time.valueOf(obj.toString()).getTime();
            case DATETIME:
                return obj;
            case FLOAT:
            case DOUBLE:
                return Double.valueOf(obj.toString());
            case BOOLEAN:
                return Boolean.valueOf(obj.toString());
        }
        return obj;
    }


    public MatchMode getMatchMode() {
        return matchMode;
    }

    public void setMatchMode(MatchMode matchMode) {
        this.matchMode = matchMode;
    }

    public CompressType getCompressType() {
        return compressType;
    }

    public void setCompressType(CompressType compressType) {
        this.compressType = compressType;
    }

    public FileType getFileType() {
        return fileType;
    }

    public void setFileType(FileType fileType) {
        this.fileType = fileType;
    }

    public String getCharsetName() {
        return charsetName;
    }

    public void setCharsetName(String charsetName) {
        this.charsetName = charsetName;
    }

    public String getBlockPath() {
        return blockPath;
    }

    public void setBlockPath(String blockPath) {
        this.blockPath = blockPath;
    }

    public String getLineDelimiter() {
        return lineDelimiter;
    }

    public void setLineDelimiter(String lineDelimiter) {
        this.lineDelimiter = lineDelimiter;
    }

    public String getFieldDelimiter() {
        return fieldDelimiter;
    }

    public void setFieldDelimiter(String fieldDelimiter) {
        this.fieldDelimiter = fieldDelimiter;
    }


    public boolean isBatchWriteMode() {
        return isBatchWriteMode;
    }

    public void setBatchWriteMode(boolean batchWriteMode) {
        isBatchWriteMode = batchWriteMode;
    }

    public int getBatchWriteSize() {
        return batchWriteSize;
    }

    public void setBatchWriteSize(int batchWriteSize) {
        this.batchWriteSize = batchWriteSize;
    }
}
