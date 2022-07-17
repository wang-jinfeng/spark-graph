package com.jinfeng.utils;

import org.apache.commons.lang.StringUtils;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @package: com.jinfeng.common.utils
 * @author: wangjf
 * @date: 2020/10/19
 * @time: 11:41 上午
 * @email: jinfeng.wang@mobvista.com
 * @phone: 152-1062-7698
 */
public class DateUtils {

    public static String format(Date date, String pattern) {
        if (date == null) {
            return null;
        }
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        return sdf.format(date);
    }

    public static String format(String date, String pattern) {
        Date now = parse(date, pattern);
        return format(now, pattern);
    }

    public static Date parse(String date, String pattern) {
        try {
            if (date == null) {
                return null;
            }
            SimpleDateFormat sdf = new SimpleDateFormat(pattern);
            return sdf.parse(date);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }


    /**
     * 获取指定日期，前n天或后n天
     *
     * @param date 指定日期
     * @param days 指定天数，负数为前n天，整数位后n天
     * @return
     */
    public static Date getDay(Date date, int days) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.DATE, calendar.get(Calendar.DATE) + days);
        return calendar.getTime();
    }


    /**
     * 获取指定日期，前n天或后n天
     *
     * @param date    指定日期
     * @param pattern 时间格式表达式
     * @param days    指定天数，负数为前n天，整数位后n天
     * @return
     */
    public static String getDay(Date date, String pattern, int days) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.DATE, calendar.get(Calendar.DATE) + days);
        return format(calendar.getTime(), pattern);
    }


    /**
     * 获取指定日期，前n天或后n天
     *
     * @param date    指定日期
     * @param pattern 时间格式表达式
     * @param days    指定天数，负数为前n天，整数位后n天
     * @return
     */
    public static Date getDay(String date, String pattern, int days) {
        Date now = parse(date, pattern);
        return getDay(now, days);
    }

    /**
     * 获取指定日期，前n天或后n天
     *
     * @param date    指定日期
     * @param pattern 时间格式表达式
     * @param days    指定天数，负数为前n天，整数位后n天
     * @return
     */
    public static String getDayByString(String date, String pattern, int days) {
        Date now = parse(date, pattern);
        return format(getDay(now, days), pattern);
    }


    /**
     * 获取指定日期前n月或后n月日期
     *
     * @param date
     * @param pattern
     * @param months
     * @return
     */
    public static String getMonthDay(Date date, String pattern, int months) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.MONTH, calendar.get(Calendar.MONTH) + months);
        return format(calendar.getTime(), pattern);
    }


    /**
     * 获取指定日期前n月或后n月日期
     *
     * @param date
     * @param pattern
     * @param months
     * @return
     */
    public static String getMongthDay(String date, String pattern, int months) {
        Date now = parse(date, pattern);
        return getMonthDay(now, pattern, months);
    }

    /**
     * 替换路径中的日期表达式
     * <p>
     * 例如：<br/>
     * 原字符串：/dw_mkt/auto/ods/event/[yyyy/MM/dd/HH]/table/[yyyy/MM/dd/]<br/>
     * 转换后字符串： /dw_mkt/auto/ods/event/2015/12/23/18/table//2015/12/23/
     * </p>
     *
     * @param path
     * @param date
     * @param datePattern
     * @return
     */
    public static String replaceDateFormat(String path, String date, String datePattern) {
        if (!StringUtils.isEmpty(path)) {
            Date now = DateUtils.parse(date, datePattern);
            Pattern pattern = Pattern.compile("\\[[^\\]]*\\]");
            Matcher matcher = pattern.matcher(path);
            while (matcher.find()) {
                String dateStr = matcher.group();
                String dateFormat = dateStr.replaceAll("\\[", "").replaceAll("\\]", "");
                path = path.replace(dateStr, DateUtils.format(now, dateFormat));
            }
        }
        return path;
    }

    /**
     * 获取两个日期之间的所有日期，包含开始和结束时间
     *
     * @param startDay
     * @param stopDay
     * @param pattern
     * @param step
     * @return
     */
    public static List<String> getRangeDay(String startDay, String stopDay, String pattern, int step) {
        return getRangeDay(startDay, stopDay, null, pattern, step);
    }

    /**
     * 获取两个日期之间的所有日期，包含开始和结束时间
     *
     * @param startDay
     * @param stopDay
     * @param pattern
     * @param step
     * @return
     */
    public static List<String> getRangeDay(String startDay, String stopDay, String maxDay, String pattern, int step) {
        step = step == 0 ? 1 : step;
        if (!StringUtils.isEmpty(maxDay)) {
            stopDay = stopDay.compareTo(maxDay) < 0 ? stopDay : maxDay;
        }
        List<String> days = new ArrayList<String>();
        while (startDay.compareTo(stopDay) <= 0) {
            days.add(startDay);
            startDay = getDayByString(startDay, pattern, step);
        }
        return days;
    }
}
