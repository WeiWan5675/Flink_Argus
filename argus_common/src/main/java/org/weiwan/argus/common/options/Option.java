package org.weiwan.argus.common.options;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/14 20:05
 * @Package: org.weiwan.argus.common.options
 * @ClassName: Option
 * @Description:
 **/
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface Option {
    String value();
}
