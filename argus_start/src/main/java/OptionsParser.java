import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.Options;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/14 19:48
 * @Package: PACKAGE_NAME
 * @ClassName: OptionsParser
 * @Description:
 **/
public class OptionsParser {

    private BasicParser parser;

    private String[] args;

    public OptionsParser(String[] args) {
        this.parser = new BasicParser();
        this.args = args;
    }

    public void parse(Options options) {

    }
}
