package org.weiwan.argus.core.pub.output.hdfs;

import org.weiwan.argus.core.constants.ArgusConstans;

import java.util.Arrays;
import java.util.List;

/**
 * @Author: xiaozhennan
 * @Date: 2020/8/7 10:54
 * @Package: org.weiwan.argus.core.pub.output.hdfs
 * @ClassName: ColumnType
 * @Description:
 **/
public enum ColumnType {

    /**
     * string type
     */
    STRING, VARCHAR, VARCHAR2, CHAR, NVARCHAR, TEXT, KEYWORD, BINARY,

    /**
     * number type
     */
    INT, INT32, MEDIUMINT, TINYINT, DATETIME, SMALLINT, BIGINT, LONG, INT64, SHORT, INTEGER,

    /**
     * double type
     */
    DOUBLE, FLOAT,
    BOOLEAN,

    /**
     * date type
     */
    DATE, TIMESTAMP, TIME,
    DECIMAL, YEAR, BIT;

    public static List<ColumnType> TIME_TYPE = Arrays.asList(
            DATE, DATETIME, TIME, TIMESTAMP
    );

    public static List<ColumnType> NUMBER_TYPE = Arrays.asList(
            INT, INTEGER, MEDIUMINT, TINYINT, SMALLINT, BIGINT, LONG, SHORT, DOUBLE, FLOAT, DECIMAL
    );

    public static ColumnType fromString(String type) {
        if (type == null) {
            throw new RuntimeException("null ColumnType!");
        }

        if (type.contains(ArgusConstans.LEFT_PARENTHESIS_SYMBOL)) {
            type = type.substring(0, type.indexOf(ArgusConstans.LEFT_PARENTHESIS_SYMBOL));
        }

        return valueOf(type.toUpperCase());
    }

    public static ColumnType getType(String type) {
        if (type.contains(ArgusConstans.LEFT_PARENTHESIS_SYMBOL)) {
            type = type.substring(0, type.indexOf(ArgusConstans.LEFT_PARENTHESIS_SYMBOL));
        }
        if (type.contains(ArgusConstans.POINT_SYMBOL)) {
            type = type.substring(type.lastIndexOf(ArgusConstans.POINT_SYMBOL) + 1, type.length());
        }

        if (type.toLowerCase().contains(ColumnType.TIMESTAMP.name().toLowerCase())) {
            return TIMESTAMP;
        }

        for (ColumnType value : ColumnType.values()) {
            if (type.equalsIgnoreCase(value.name())) {
                return value;
            }
        }

        return ColumnType.STRING;
    }

    public static boolean isTimeType(String type) {
        return TIME_TYPE.contains(getType(type));
    }

    public static boolean isNumberType(String type) {
        return NUMBER_TYPE.contains(getType(type));
    }
}
