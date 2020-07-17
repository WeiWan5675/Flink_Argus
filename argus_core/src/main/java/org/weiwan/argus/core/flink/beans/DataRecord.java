package org.weiwan.argus.core.flink.beans;

import java.io.Serializable;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/2 11:11
 * @Package: com.hopson.dc.flink.common.beans
 * @ClassName: DataRecord
 * @Description:
 **/
public class DataRecord<T> implements Serializable {
    private String pk;
    private String tableName;
    private String timestmp;
    private String schema;
    private T data;

    public DataRecord(T value) {
        this.data = value;
    }

    public DataRecord() {


    }

    public String getPk() {
        return pk;
    }

    public void setPk(String pk) {
        this.pk = pk;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTimestmp() {
        return timestmp;
    }

    public void setTimestmp(String timestmp) {
        this.timestmp = timestmp;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }


    @Override
    public String toString() {
        return "DataRecord{" +
                "pk='" + pk + '\'' +
                ", tableName='" + tableName + '\'' +
                ", timestmp='" + timestmp + '\'' +
                ", data=" + data +
                '}';
    }


    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }
}
