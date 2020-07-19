package org.weiwan.argus.start.enums;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/14 20:13
 * @Package: org.weiwan.argus.common.options
 * @ClassName: org.weiwan.argus.start.enums.JobMode
 * @Description:
 **/
public enum JobMode {

    local("Local", "本地Main方法模式"),
    yarnpre("YarnPer", "在yarn上启动flinkSession运行"),
    yarn("Yarn", "在Yarn上已经启动的Session中允许"),
    standalone("Standalone", "本地Standalone集群模式");
    private String mode;
    private String msg;


    JobMode(String mode, String msg) {
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
