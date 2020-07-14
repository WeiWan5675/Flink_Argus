package org.weiwan.argus.common.options;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/14 20:04
 * @Package: org.weiwan.argus.common.options
 * @ClassName: DemoOptions
 * @Description:
 **/
@Option("demoOptions")
public class DemoOptions {

    @OptionRequired(description = "运行模式")
    private String MODE = JobMode.LocalMode.name();


}
