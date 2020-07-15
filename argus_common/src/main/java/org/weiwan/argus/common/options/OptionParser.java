/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.weiwan.argus.common.options;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;


import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * The Parser of Launcher commandline options
 * <p>
 * Company: www.dtstack.com
 *
 * @author huyifan.zju@163.com
 */
public class OptionParser {

    private Options options = new Options();

    private BasicParser parser = new BasicParser();

    private Class<?> optionTemplate;

    private CommandLine commandLine;

    private String[] args;
    private boolean inited;


    public OptionParser(String[] args) {
        this.args = args;
    }


    public <E> E parse(Class<E> eClass) throws Exception {
        E e = null;
        try {
            if (!inited) {
                initOptions(eClass);
            }

            e = (E) optionTemplate.newInstance();
            Field[] fields = e.getClass().getDeclaredFields();
            for (Field field : fields) {
                OptionField optionField = field.getAnnotation(OptionField.class);
                //兼容字段名字的形式,和简写Key
                String[] optionNames = optionField.oweKeys();
                String fieldName = field.getName();
                String value = null;
                for (String optionName : optionNames) {
                    value = commandLine.getOptionValue(optionName);
                    if (value != null) {
                        break;
                    }
                }

                if (value == null) {
                    value = commandLine.getOptionValue(fieldName);
                }

                //如果参数不能为空,但是命令行为空,抛出异常
                if (optionField != null) {
                    if (optionField.required() && StringUtils.isBlank(value)) {
                        throw new RuntimeException(String.format("parameters of %s is required", fieldName));
                    }
                }

                //给反射的对象设置属性
                field.setAccessible(true);
                if (StringUtils.isBlank(value)) {
                    value = optionField.defaultValue();
                    if (value == null || value.equalsIgnoreCase("")) {
                        //默认值为空 TODO 默认值为空,这里需要拿到实体类的默认值
                        Object fieldValue = getFieldValue(e, field);
                        if (fieldValue != null) {
                            value = String.valueOf(fieldValue);
                        }
                    }
                }
                field.set(e, value);
            }
        } catch (Exception exception) {
            exception.printStackTrace();
            throw exception;
        }
        return e;
    }

    private <E> void initOptions(Class<E> eClass) throws Exception {
        this.optionTemplate = eClass;
        Field[] fields = optionTemplate.getDeclaredFields();
        Option annotation = optionTemplate.getAnnotation(Option.class);
        if (annotation == null) {
            throw new RuntimeException(String.format(" option Class %s must haveing @Option annotation", optionTemplate.getName()));
        }
        for (Field field : fields) {
            String fieldName = field.getName();
            OptionField optionField = field.getAnnotation(OptionField.class);
            if (optionField != null) {
                String opKey = null;
                String[] oweKeys = optionField.oweKeys();
                for (String oweKey : oweKeys) {
                    opKey = oweKey;
                    if (opKey != null) {
                        options.addOption(opKey, optionField.hasArg(), optionField.description());
                    }
                }
                if (opKey == null) {
                    opKey = fieldName;
                    options.addOption(opKey, optionField.hasArg(), optionField.description());
                }
            }
        }
        this.commandLine = parser.parse(this.options, args, true);
        inited = true;
    }


    public static Object getFieldValue(Object object, Field field) throws Exception {
        if (field.getGenericType().toString().equals(
                "class java.lang.String")) { // 如果type是类类型，则前面包含"class "，后面跟类名
            // 拿到该属性的gettet方法
            /**
             * 这里需要说明一下：他是根据拼凑的字符来找你写的getter方法的
             * 在Boolean值的时候是isXXX（默认使用ide生成getter的都是isXXX）
             * 如果出现NoSuchMethod异常 就说明它找不到那个gettet方法 需要做个规范
             */
            Method m = (Method) object.getClass().getMethod(
                    "get" + getMethodName(field.getName()));
            String val = (String) m.invoke(object);// 调用getter方法获取属性值
            if (val != null) {
                return val;
            }
        }
        return null;
    }


    // 把一个字符串的第一个字母大写、效率是最高的、
    private static String getMethodName(String fildeName) throws Exception {
        byte[] items = fildeName.getBytes();
        items[0] = (byte) ((char) items[0] - 'a' + 'A');
        return new String(items);
    }
}
