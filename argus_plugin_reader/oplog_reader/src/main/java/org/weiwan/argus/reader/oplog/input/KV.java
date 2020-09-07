package org.weiwan.argus.reader.oplog.input;

import java.io.Serializable;

/**
 * @Author: lsl
 * @Date: 2020/8/26 16:08
 * @Description:
 **/
public class KV implements Serializable {

    private String key;
    private Object value;

    public KV(String key, Object value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }
}
