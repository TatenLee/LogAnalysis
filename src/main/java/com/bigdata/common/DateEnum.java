package com.bigdata.common;

/**
 * @Author Taten
 * @Description
 **/
public enum DateEnum {
    YEAR("year"),
    SEASON("season"),
    MONTH("month"),
    WEEK("week"),
    DAY("day"),
    HOUR("hour");

    public final String dateType;

    DateEnum(String dateType) {
        this.dateType = dateType;
    }

    /**
     * 根据type获取时间枚举
     *
     * @param dateType
     * @return
     */
    public static DateEnum valueOfDate(String dateType) {
        for (DateEnum value : values()) {
            if (dateType.equals(value.dateType)) {
                return value;
            }
        }
        throw new RuntimeException("no support for this type of date enumeration. dateType: " + dateType);
    }
}
