package com.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * 时间工具表
 */
public class DateUtils {

    /**
     * 获取当前时间
     * @return
     */
    public Date currentTime(){
        //设置日期格式
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        // new Date()为获取当前系统时间
        String time = df.format(new Date());
        Date currentTime = null;
        try{
            currentTime= df.parse(time);
        }catch (ParseException e){
            e.printStackTrace();
        }
        return currentTime;
    }

    /**
     * 获取时间的时间戳
     * @param time
     * @return timestamp
     */
    public Long getTimestamp(String time) {

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long timestamp = 0L;
        Date date = null;

        try {
            date = simpleDateFormat.parse(time);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        timestamp = date.getTime();
        return timestamp;

    }

    /**
     * 增加n天后时间
     * @return
     */
    public Date getAddDays(int n){
        //设置日期格式
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date addTime = null;
        // 获取当前时间
        Date date = new Date();
        Calendar calendar = Calendar.getInstance();
        // 增加n天
        calendar.add(Calendar.DATE, n);

        String time = df.format(calendar.getTime());
        try {
            addTime = df.parse(time);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return addTime;
    }


    public Date StrToDate(String str){
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = null;
        try {
            date = format.parse(str);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return date;
    }

    public static void main(String[] args) {

        //设置日期格式
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date addTime = null;
        // 获取当前时间
        Date date = new Date();
        System.out.println("date: " + df.format(date));
        Calendar calendar = Calendar.getInstance();
        // 增加n天
        calendar.add(Calendar.DATE, 1);
        System.out.println("addDate: " + df.format(calendar.getTime()));


//        //设置日期格式
//        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        Date addTime = null;
//        // 获取当前时间
//        Date date = new Date();
//        Calendar calendar = Calendar.getInstance();
//        // 增加n天
//        calendar.add(Calendar.DATE, n);
//
//        String time = df.format(calendar.getTime());
//        try {
//            addTime = df.parse(time);
//        } catch (ParseException e) {
//            e.printStackTrace();
//        }
//        return addTime;
    }

}
