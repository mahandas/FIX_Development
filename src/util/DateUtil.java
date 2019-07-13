package util;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import client.ConfigReader;

public class DateUtil {
    public static final String FIX_DATE_FORMAT = "yyyyMMdd";
    public static final String DB_DATE_FORMAT = "yyyy-MM-dd";		
    public static final String DB_DATE_TIME_FORMAT = "dd-MMM-yyyy HH:mm:ss";
    public static final String FIX_DATE_TIME_FORMAT = "yyyyMMdd-HH:mm:ss";
    public static final String LOG_DATE_TIME_FORMAT = "dd_MMM_yyyy HH_mm_ss";

    /**
     * @param date
     * @param format
     * @param requiredFormat
     * @return
     */
    public static String convertDateFormat(String date, String format, String requiredFormat) {
        if (date == null)
            return "";
        else if (date.isEmpty())
            return "";
        return DateUtil.dateToString(DateUtil.stringToDate(date, format), requiredFormat);
    }

    /**
     * @param date
     * @param requiredFormat
     * @return
     */
    public static String convertDateFormat(String date, String requiredFormat) {
        return convertDateFormat(date, FIX_DATE_FORMAT, requiredFormat);
    }

    /**
     * @param date
     * @return
     */
    public static String convertDateFormat(String date) {
        return convertDateFormat(date, FIX_DATE_FORMAT, DB_DATE_FORMAT);
    }

    /**
     * @param date
     * @param format
     * @return
     */
    public static String dateToString(Date date, String format) {
        if (date == null)
            return "";
        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        return dateFormat.format(date);
    }

    /**
     * @param text
     * @param format
     * @return
     */
    public static Date stringToDate(String text, String format) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        try {
            return dateFormat.parse(text);
        } catch (ParseException e) {
            return null;
        }
    }

    /**
     * @return
     */
    public static String getCurrentDateTime_Str() {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.SECOND,Integer.parseInt(ConfigReader.getConfigFile().getProperty("ExpiryTime","30")));  
        SimpleDateFormat sdf = new SimpleDateFormat(FIX_DATE_TIME_FORMAT);
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        return sdf.format(cal.getTime());
    }

    /**
     * @return
     * @throws ParseException
     */
    public static Date getCurrentDateTime() throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(FIX_DATE_TIME_FORMAT);
        return sdf.parse(getCurrentDateTime_Str());
    }

    /**
     * @return
     * @throws ParseException
     */
    public static String getLogCurrentDateTime() throws ParseException {
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat(LOG_DATE_TIME_FORMAT);
        return sdf.format(cal.getTime());
    }
}
