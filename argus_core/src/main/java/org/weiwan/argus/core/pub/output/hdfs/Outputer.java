package org.weiwan.argus.core.pub.output.hdfs;

import java.util.Map;

/**
 * @Author: xiaozhennan
 * @Date: 2020/8/5 17:52
 * @Package: org.weiwan.argus.core.pub.output.hdfs
 * @ClassName: Outputer
 * @Description:
 **/
public interface Outputer {

    public void initOutputTypes(Map<String, String> typeMap);

}
