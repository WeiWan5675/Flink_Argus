package org.weiwan.argus.start.enums;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/14 20:13
 * @Package: org.weiwan.argus.common.options
 * @ClassName: org.weiwan.argus.start.enums.RunMode
 * @Description:
 **/
public enum RunMode {

    local("Local", "本地Main方法模式"),
    yarnper("YarnPer", "在yarn上启动flinkSession运行"),
    yarn("Yarn", "在Yarn上已经启动的Session中运行"),
    standalone("Standalone", "本地Standalone集群模式"),
    application("application","Flink 1.11 新模式" );
    private String mode;
    private String msg;


    RunMode(String mode, String msg) {
        this.mode = mode;
        this.msg = msg;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }
}
