package com.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * 获取配置文件属性
 * @Author: liugf
 * @Date: 2020/8/11 上午9:24
 */
public class FilePropertiesMapModelUpdate {

    private static final Logger logger = LoggerFactory.getLogger(FilePropertiesMapModelUpdate.class);

    // topic 名称
    String topicName;
//    // 分隔符
//    String splitSign;
//    // 列数
//    int columnslength;
////     所有列名称
////    String[] columns;

    // 定义map
    HashMap<String, String> mappropertis;

    Map<String, HashMap<String, String> > mapprtopics;

    public FilePropertiesMapModelUpdate(String line) {
        // 通过逗号分隔
        String[] allColumn = line.split(",");

        setTopicName(allColumn[0]);

        mapprtopics = new HashMap<String, HashMap<String, String>>();

        mappropertis = new HashMap<String, String>();
        for (int i = 1; i < allColumn.length; i++) {
            // 先转成小写，再通过":"分隔出来
            String[] column = allColumn[i].split(":");
//                    .toLowerCase()
            // 将列名和数据类型存入map中
//            System.out.println("column[0]: " + column[0]);
//            System.out.println("column[1]: " + column[1]);
            mappropertis.put(column[0], column[1]);
        }

        mapprtopics.put(allColumn[0], mappropertis);

//
//        mapprtopics = new HashMap<String, HashMap<String, String>>();
//        for (int i = 1; i < allColumn.length; i++) {
//
//            String[] column = allColumn[i].split(":");
//
////            mapprtopics.put(allColumn[0],HashMap<column[0], column[1]>);
//
//        }

    }

//    public FilePropertiesMapModel(String line) {
//        // 通过逗号分隔
////        logger.info("通过逗号分隔每行.........");
//        String[] allColumn = line.split(",");
////        System.out.println("allColumn: " + allColumn.toString());
////        logger.info("打印通过逗号分隔后的长度allColumn.length：" + allColumn.length);
//
//        setTopicName(allColumn[0]);
////        System.out.println(allColumn[0]);
////        logger.info("topic名称：" + topicName);
//
////        setColumnslength(allColumn.length - 1);
////        logger.info("获取topic字段中的列数量: " + columnslength);
//
//        mapprtopics = new HashMap<String, HashMap<String, String>>();
//
//        mappropertis = new HashMap<String, String>();
////        String topicColumn = "";
//        for (int i = 1; i < allColumn.length; i++) {
//            // 先转成小写，再通过":"分隔出来
//            String[] column = allColumn[i].split(":");
////                    .toLowerCase()
//            // 将列名和数据类型存入map中
////            System.out.println("column[0]: " + column[0]);
////            System.out.println("column[1]: " + column[1]);
//            mappropertis.put(column[0], column[1]);
//        }
//
//        mapprtopics.put(allColumn[0], mappropertis);
//
////
////        mapprtopics = new HashMap<String, HashMap<String, String>>();
////        for (int i = 1; i < allColumn.length; i++) {
////
////            String[] column = allColumn[i].split(":");
////
//////            mapprtopics.put(allColumn[0],HashMap<column[0], column[1]>);
////
////        }
//
//    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public HashMap<String, String> getMappropertis() {
        return mappropertis;
    }

    public void setMappropertis(HashMap<String, String> mappropertis) {
        this.mappropertis = mappropertis;
    }

    public Map<String, HashMap<String, String>> getMapprtopics() {
        return mapprtopics;
    }

    public void setMapprtopics(Map<String, HashMap<String, String>> mapprtopics) {
        this.mapprtopics = mapprtopics;
    }

    public static void main(String[] args) {

        String line = "tp_amr_e_mp_vol_curve_test,id:decimal,data_date:timestamp,phase_flag:decimal,data_whole_flag:string,data_point_flag:decimal,u1:decimal,u2:decimal";

        FilePropertiesMapModelUpdate filePropertiesMapModel = new FilePropertiesMapModelUpdate(line);

        System.out.println("........");


    }

}
