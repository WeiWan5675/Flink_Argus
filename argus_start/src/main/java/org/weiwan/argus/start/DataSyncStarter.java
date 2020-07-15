package org.weiwan.argus.start;

import org.weiwan.argus.core.ArgusRun;
import org.weiwan.argus.common.options.OptionParser;
import org.weiwan.argus.start.config.StartOptions;
import org.weiwan.argus.start.enums.JobMode;


/**
 * @Author: xiaozhennan
 * @Date: 2020/7/14 19:24
 * @Package: PACKAGE_NAME
 * @ClassName: org.weiwan.argus.start.DataSyncStarter
 * @Description:
 **/
public class DataSyncStarter {

    public static void main(String[] args) throws Exception {
        args = new String[]{
                "--mode", "Local",
                "--aconf", "argus-default.yaml",
//                "--hconf","hadoop-cmd.yaml"
        };
        OptionParser optionParser = new OptionParser(args);
        StartOptions options = optionParser.parse(StartOptions.class);

        String mode = options.getMode();
        boolean startFlag = false;
        switch (JobMode.valueOf(mode)) {
            case Local:
                System.out.println("运行模式:" + JobMode.Local.toString());
                startFlag = startFromLocalMode(options);
                break;
            case Standalone:
                System.out.println("运行模式:" + JobMode.Standalone.toString());
                startFlag = startFromStandaloneMode(options);
                break;
            case Yarn:
                System.out.println("运行模式:" + JobMode.Yarn.toString());
                startFlag = startFromYarnMode(options);
                break;
            case YarnPer:
                System.out.println("运行模式:" + JobMode.YarnPer.toString());
                startFlag = startFromYarnPerMode(options);
                break;
            default:
                System.out.println("没有匹配的运行模式!");
        }

        System.out.println(startFlag);


    }

    private static boolean startFromYarnPerMode(StartOptions options) {
        return false;
    }

    private static boolean startFromYarnMode(StartOptions options) {

        return false;
    }

    private static boolean startFromLocalMode(StartOptions options) throws Exception {
        //转化脚本启动的options为Main方法可以识别的参数
        //pluginPath

        //ReaderPluginName
        //WriterPluginName

        //ReaderPluginClassName
        //WriterPluginClassName


        //Flink自身运行的一些参数

        //任务的配置文件

        //

        ArgusRun.main(new String[]{});
        return false;
    }

    private static boolean startFromStandaloneMode(StartOptions options) {

        return false;
    }

}
