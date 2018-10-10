package com.bigdata.util;

import com.bigdata.common.DateEnum;
import org.apache.log4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Author Taten
 * @Description 操作时间工具方法
 **/
public class TimeUtil {
    private static final Logger logger = Logger.getLogger(TimeUtil.class);
    private static String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";

    /**
     * 验证日期是否合法
     *
     * @param date
     * @return
     */
    public static boolean isValidateDate(String date) {
        Matcher matcher = null;
        boolean res = false;
        String regexp = "^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}";
        if (null != date) {
            Pattern pattern = Pattern.compile(regexp);
            matcher = pattern.matcher(date);
        }
        if (null != matcher) {
            res = matcher.matches();
        }
        return res;
    }

    /**
     * 获取昨天的日期,不传格式为默认的
     *
     * @return
     */
    public static String getYesterdayDate() {
        return getYesterdayDate(DEFAULT_DATE_FORMAT);
    }

    /**
     * 获取昨天的日期
     *
     * @return
     */
    public static String getYesterdayDate(String dateformat) {
        SimpleDateFormat sdf = new SimpleDateFormat(dateformat);
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DAY_OF_YEAR, -1);
        return sdf.format(calendar.getTime());
    }


    /**
     * 将字符串的日期转换成时间戳, 默认的格式
     *
     * @param date
     * @return
     */
    public static long parserString2Long(String date) {
        return parserString2Long(date, DEFAULT_DATE_FORMAT);
    }

    /**
     * 将字符串的日期转换成时间戳 2018-07-26  15127398848923
     *
     * @param date
     * @return
     */
    public static long parserString2Long(String date, String parttern) {
        SimpleDateFormat sdf = new SimpleDateFormat(parttern);
        Date dt = null;
        try {
            dt = sdf.parse(date);
        } catch (ParseException e) {
            logger.warn("Exception in parsing string to long of date", e);
        }
        return dt == null ? 0 : dt.getTime();
    }


    /**
     * 将时间戳转换成日期, 默认的格式
     *
     * @param time
     * @return
     */
    public static String parserLong2String(long time) {
        return parserLong2String(time, DEFAULT_DATE_FORMAT);
    }

    /**
     * 将时间戳转换为指定格式的日期
     *
     * @param time
     * @param format
     * @return
     */
    public static String parserLong2String(long time, String format) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        return new SimpleDateFormat(format).format(calendar.getTime());
    }

    /***
     * 根据时间戳获取时间相关信息
     * @param time
     * @param type
     * @return
     */
    public static int getDateInfo(long time, DateEnum dateType) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        if (dateType.equals(DateEnum.YEAR)) {
            return calendar.get(Calendar.YEAR);
        }
        if (dateType.equals(DateEnum.SEASON)) {
            int month = calendar.get(Calendar.MONTH) + 1;
            return (month + 2) / 3;
        }
        if (dateType.equals(DateEnum.MONTH)) {
            return calendar.get(Calendar.MONTH) + 1;
        }
        if (dateType.equals(DateEnum.WEEK)) {
            return calendar.get(Calendar.WEEK_OF_YEAR);
        }

        if (dateType.equals(DateEnum.DAY)) {
            return calendar.get(Calendar.DAY_OF_MONTH);
        }
        if (dateType.equals(DateEnum.HOUR)) {
            return calendar.get(Calendar.HOUR_OF_DAY);
        }
        throw new RuntimeException("No support for the type of date. dateType: " + dateType.dateType);
    }

    /**
     * 根据时间戳获取时间戳所在周第一天的时间戳
     *
     * @param time
     * @return
     */
    public static long getFirstDayOfWeek(long time) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        calendar.set(Calendar.DAY_OF_WEEK, 1);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar.getTimeInMillis();
    }
}
