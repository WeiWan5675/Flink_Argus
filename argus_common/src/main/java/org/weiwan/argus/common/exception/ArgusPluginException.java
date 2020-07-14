package org.weiwan.argus.common.exception;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/14 16:54
 * @Package: org.weiwan.argus.common.exception
 * @ClassName: ArgusPluginException
 * @Description:
 **/
public class ArgusPluginException extends ArgusCommonException {
    private String pluginName;

    public String getPluginName() {
        return pluginName;
    }

    public void setPluginName(String pluginName) {
        this.pluginName = pluginName;
    }


}
