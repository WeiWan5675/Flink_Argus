package org.weiwan.argus.common.exception;

import com.alibaba.fastjson.JSONObject;
import org.weiwan.argus.common.enums.ArgusExceptionEnum;

/**
 * @Author: xiaozhennan
 * @Date: 2020/4/27 17:24
 * @Package: com.hopson.dc.realtime.common.exception
 * @ClassName: ArgusCommonException
 * @Description:
 **/
public class ArgusCommonException extends RuntimeException {
    public String code;
    public String msg;

    public ArgusCommonException(ArgusExceptionEnum exceptionEnum) {
        this.code = exceptionEnum.getCode();
        this.msg = exceptionEnum.getMsg();
    }

    public ArgusCommonException(Throwable e) {
        super(e);
    }

    public ArgusCommonException() {
    }

    public ArgusCommonException(ArgusExceptionEnum argusEnum, String msg) {
        super(msg);
        this.code = argusEnum.getCode();
        this.msg = msg;
    }

    public ArgusCommonException(String msg) {
        super(msg);
        this.code = ArgusExceptionEnum.SYS_UNKNOWN_ERROR.getCode();
        this.msg = msg;
    }


    public static ArgusCommonException generateParameterIsNullException(String msg) {
        return new ArgusCommonException(ArgusExceptionEnum.AP_PARAM_NULL, msg);
    }


    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public JSONObject converToJson() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("code", this.getCode());
        jsonObject.put("msg", this.getMsg());
        return jsonObject;
    }
}
