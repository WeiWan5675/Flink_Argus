package org.weiwan.argus.common.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

/**
 * @Date: 2018/8/23 11:14
 * @Author: xiaozhennan
 * @Package: com.ipaynow.dc.common.utils
 * @ClassName: DateUtils
 * @Description:
 **/
public class DateUtils {
    public static final String SDF_YYYYMMDDHHMMSS = "yyyyMMddHHmmss";
    public static final String TRANS_TIME = "yyyy-MM-dd HH:mm:ss";
    public static final String SDF_YYYY_MM_DD_HH_MM_SS = "yyyy-MM-dd HH:mm:ss";
    public static final String SDF_YYYY_MM_DD = "yyyyMMdd";
    public static final String SDF_YYYY_MM = "yyyyMM";
    public static final String RM_SDF_YYYY_MM_DD = "yyyy-MM-dd";

    //30*24*60=432000 一个月的分钟数
    public static final Integer LIMITENDTIME = 43200;

    public static String getNowDateStr() {
        return getDateStr(new Date());
    }

    public static String getDateStr(Date date) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(SDF_YYYYMMDDHHMMSS);
        return dateFormat.format(date);
    }
    public static String getDateStrYMDHMS(Date date) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(SDF_YYYY_MM_DD_HH_MM_SS);
        return dateFormat.format(date);
    }


    public static Date getTodayOriginDate() {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
//        cal.add(Calendar.DAY_OF_MONTH, 1); //当前时间加1天
        return cal.getTime();
    }

    /**
     * 用于计算当前时间加上步长
     *
     * @param step 步长
     * @return
     */
    public static Date dateFormat(int step) {
        Date date;
        Calendar cl = Calendar.getInstance();
        cl.setTime(new Date());
        cl.add(Calendar.MINUTE, step);
        date = cl.getTime();
        return date;
    }

    public static Date getTimeout(Date startTime, int step) {
        Date date;
        Calendar cl = Calendar.getInstance();
        cl.setTime(startTime);

        cl.add(Calendar.SECOND, step);
        date = cl.getTime();

        return date;

    }

    public static Date dateFormatForSecond(int step) {
        Date date;
        Calendar cl = Calendar.getInstance();
        cl.setTime(new Date());
        cl.add(Calendar.SECOND, step);
        date = cl.getTime();
        return date;
    }

    public static int getCurrentBySeconds() {
        return (int) (System.currentTimeMillis() / 1000);
    }

    public static int getDateSeconds(Date date) {

        return (int) (date.getTime() / 1000);
    }

    /**
     * 获取d之前day的时间
     *
     * @param d
     * @param day
     * @return
     */
    public static Date getDateBefore(Date d, int day) {
        Calendar now = Calendar.getInstance();
        now.setTime(d);
        now.set(Calendar.DATE, now.get(Calendar.DATE) - day);
        return now.getTime();
    }

    public static Date getMinuteBefore(Date d, int minute) {
        Calendar now = Calendar.getInstance();
        now.setTime(d);
        now.add(Calendar.MINUTE, -minute);
        return now.getTime();
    }


    /**
     * 限制查询间隔，不超过30天。
     *
     * @param startTime
     * @return
     */
    public static Integer limitQueryTime(Integer startTime, Integer endTime) {
        //开始时间加上30天的时间间隔作为结束时间
        if (endTime - startTime > LIMITENDTIME) {
            endTime = startTime + LIMITENDTIME;
        }
        return endTime;
    }

    public static Date converStringToDate(String dateStr, String format) {

        Date date = new Date();
        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat(format);
            date = dateFormat.parse(dateStr);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return date;
    }

    public static Date formTransTime(String value) {

        Date date = new Date();
        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat(TRANS_TIME);
            date = dateFormat.parse(value);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return date;
    }

    public static Date formWXPayTime(String value) {

        Date date = new Date();
        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat(SDF_YYYYMMDDHHMMSS);
            date = dateFormat.parse(value);
        } catch (ParseException e) {
//            e.printStackTrace();
        }
        return date;
    }

    public static Date formRMTime(String value) {

        Date date = new Date();
        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat(RM_SDF_YYYY_MM_DD);
            date = dateFormat.parse(value);
        } catch (ParseException e) {
//            e.printStackTrace();
        }
        return date;
    }

    public static String fromDate2RM(Date value) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(RM_SDF_YYYY_MM_DD);
        String result = dateFormat.format(value);
        return result;
    }
    public static String fromDate2TransTime(Date value) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(TRANS_TIME);
        String result = dateFormat.format(value);
        return result;
    }

    public static String fromDate2Str(Date value) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(SDF_YYYYMMDDHHMMSS);
        String result = dateFormat.format(value);
        return result;
    }

    public static String convertTimeDate(Date value){
        SimpleDateFormat dateFormat = new SimpleDateFormat(SDF_YYYY_MM_DD_HH_MM_SS);
        return dateFormat.format(value);
    }

    public static String convertTimeStr(String value) {
        Date date = new Date();
        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat(SDF_YYYYMMDDHHMMSS);
            date = dateFormat.parse(value);
        } catch (Exception e) {

        }
        SimpleDateFormat dateFormat1 = new SimpleDateFormat(SDF_YYYY_MM_DD_HH_MM_SS);
        return dateFormat1.format(date);

    }

    public static String formatDateToMonthStr(Date date) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(SDF_YYYY_MM);
        String result = dateFormat.format(date);
        return result;
    }

    public static int getIntervalDays(Date fDate, Date oDate) {

        Calendar aCalendar = Calendar.getInstance();

        aCalendar.setTime(fDate);

        int day1 = aCalendar.get(Calendar.DAY_OF_YEAR);

        aCalendar.setTime(oDate);

        int day2 = aCalendar.get(Calendar.DAY_OF_YEAR);

        return day2 - day1;

    }
    public static String dateDayFormatStr(int step) {
        Date date;
        Calendar cl = Calendar.getInstance();
        cl.setTime(new Date());
        cl.add(Calendar.DAY_OF_MONTH, step);
        date = cl.getTime();
        return formatDateToDayStr(date);
    }

    public static String formatDateToDayStr(Date date) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(SDF_YYYY_MM_DD);
        String result = dateFormat.format(date);
        return result;
    }


    //得到此刻时间，减去10分钟
    public static Date getTimeBefore60Minute(int minute, int num) {
        GregorianCalendar gc = new GregorianCalendar();
        gc.add(minute, num);
        Date beforeTime10 = gc.getTime();
        return beforeTime10;
    }

    /**
     * 日期加减操作
     *
     * @param currentDate 当前日期
     * @return
     */
    public static Date getSpecifiedDateBeforeOrAfter(Date currentDate, int seconds) {
        Calendar c = Calendar.getInstance();
        int second = c.get(Calendar.SECOND);
        c.set(Calendar.SECOND, second + seconds);
        return c.getTime();
    }

    /**
     *
     * @param d
     * @param format
     * @param rtnFormat
     * @return
     */
    public static String getDateTimeFromString(String d, String format,String rtnFormat){
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        SimpleDateFormat rtnf = new SimpleDateFormat(rtnFormat);
        try {
            return rtnf.format(sdf.parse(d));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 根据指定日期格式验证日期
     * @param date
     * @param dateFormat
     * @return
     */
    public static boolean isValidDate(String date, String dateFormat) {
        boolean convertSuccess = true;
        // 指定日期格式为四位年/两位月份/两位日期，注意yyyy/MM/dd区分大小写；
        SimpleDateFormat format = new SimpleDateFormat(dateFormat);
        try {
            // 设置lenient为false.
            // 否则SimpleDateFormat会比较宽松地验证日期，比如2007/02/29会被接受，并转换成2007/03/01
            format.setLenient(false);
            format.parse(date);
        } catch (ParseException e) {
            // e.printStackTrace();
            // 如果throw java.text.ParseException或者NullPointerException，就说明格式不对
            convertSuccess = false;
        }
        return convertSuccess;
    }

    public static boolean isSameDay(Date date1, Date date2) {
        if(date1 != null && date2 != null) {
            Calendar cal1 = Calendar.getInstance();
            cal1.setTime(date1);
            Calendar cal2 = Calendar.getInstance();
            cal2.setTime(date2);
            return isSameDay(cal1, cal2);
        } else {
            throw new IllegalArgumentException("The date must not be null");
        }
    }

    public static boolean isSameDay(Calendar cal1, Calendar cal2) {
        if(cal1 != null && cal2 != null) {
            return cal1.get(0) == cal2.get(0) && cal1.get(1) == cal2.get(1) && cal1.get(6) == cal2.get(6);
        } else {
            throw new IllegalArgumentException("The date must not be null");
        }
    }





    public static void main(String[] args) {
        String nowDateStr = getDateStr(formTransTime("2018-12-21 10:15:58"));
        System.out.println(nowDateStr);
    }
}
