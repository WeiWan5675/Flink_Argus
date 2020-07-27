package org.weiwan.argus.core.pub.pojo;

import java.io.Serializable;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/27 17:22
 * @Package: org.weiwan.argus.core.pub.pojo
 * @ClassName: DataField
 * @Description:
 **/
public class DataField implements Serializable{

    private String fieldKey;
    private String fieldType;
    private Object value;

    public DataField(String fieldKey, String fieldType, Object value) {
        this.fieldKey = fieldKey;
        this.fieldType = fieldType;
        this.value = value;
    }

    public DataField() {
    }

    public String getFieldKey() {
        return fieldKey;
    }

    public void setFieldKey(String fieldKey) {
        this.fieldKey = fieldKey;
    }

    public String getFieldType() {
        return fieldType;
    }

    public void setFieldType(String fieldType) {
        this.fieldType = fieldType;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "DataField{" +
                "fieldKey='" + fieldKey + '\'' +
                ", fieldType='" + fieldType + '\'' +
                ", value=" + value +
                '}';
    }
}
