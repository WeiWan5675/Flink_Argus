package org.weiwan.argus.pub.pojo;

import java.io.Serializable;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/14 17:52
 * @Package: org.weiwan.argus.reader.mysql.input
 * @ClassName: DataRecord
 * @Description:
 **/
public class DataRecord<T> implements Serializable {
    private String tableName;
    private String pk;
    private String timestamp;
    private String schemaName;
    private String dataPath;
    private String dataType;

    private T data;


    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getDataPath() {
        return dataPath;
    }

    public void setDataPath(String dataPath) {
        this.dataPath = dataPath;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getPk() {
        return pk;
    }

    public void setPk(String pk) {
        this.pk = pk;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }
}
