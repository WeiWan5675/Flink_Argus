import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/14 19:24
 * @Package: PACKAGE_NAME
 * @ClassName: DataSyncStarter
 * @Description:
 **/
public class DataSyncStarter {

    public static void main(String[] args) {

        //解析参数
        // 创建 Options 对象
        Options options = new Options();

        options.addOption("h",true,"test parameters");

        OptionsParser optionsParser = new OptionsParser(args);

        optionsParser.parse(options);
    }

}
