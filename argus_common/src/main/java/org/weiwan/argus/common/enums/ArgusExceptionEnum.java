package org.weiwan.argus.common.enums;

/**
 * @Author: xiaozhennan
 * @Date: 2020/4/28 9:13
 * @Package: com.hopson.dc.realtime.common.enums
 * @ClassName: ArgusExceptionEnum
 * @Description:
 **/
public enum ArgusExceptionEnum {
    //系统异常
    SYS_DB_ERROR("SYS0001", "数据库异常"),
    SYS_CONN_ERROR("SYS0002", "连接异常"),
    SYS_DATA_ERROR("SYS0003", "数据异常"),
    SYS_PARAM_ERROR("SYS0004", "参数异常"),
    SYS_UNKNOWN_ERROR("SYS9999", "未知异常"),
    SYS_ERROR("SYS9998", "系统异常"),
    FAILURE("FAILURE", "失败"),
    SUCCESS("SUSSESS", "成功"),

    //AP异常
    AP_PARAM_NULL("AP10001", "参数为空"),
    AP_PARAM_ERROR("AP10002", "参数错误"),


    //kafka消费解析的报文
    AP_KAFKA_MSG_ERROR("AP20001", "Kafka报文异常"),

    //解析异常
    CODE_ANALYZE_ERROR("CODE0001", "代码参数解析异常");


    private String code;
    private String msg;

    ArgusExceptionEnum(String code, String msg) {
        this.code = code;
        this.msg = msg;
    }

    public String getMsg() {
        return msg;
    }


    public void setMsg(String msg) {
        this.msg = msg;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }
}
