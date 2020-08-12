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
public class FilePropertiesMapModel {

    private static final Logger logger = LoggerFactory.getLogger(FilePropertiesMapModel.class);

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
    public FilePropertiesMapModel(String line) {
        // 通过逗号分隔
        String[] allColumn = line.split(",");
        setTopicName(allColumn[0]);

        mappropertis = new HashMap<String, String>();
        for (int i = 1; i < allColumn.length; i++) {
            // 通过":"分隔出来
            String[] column = allColumn[i].split(":");
//                    .toLowerCase()
            // 将列名和数据类型存入map中
//            System.out.println("column[0]: " + column[0]);
//            System.out.println("column[1]: " + column[1]);
            mappropertis.put(column[0], column[1]);
//            setMappropertis(mappropertis);
        }

    }

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

    public static void main(String[] args) {

        String line = "tp_amr_e_mp_vol_curve_test,id:decimal,data_date:timestamp,phase_flag:decimal,data_whole_flag:string,data_point_flag:decimal,u1:decimal,u2:decimal";

        FilePropertiesMapModel filePropertiesMapModel = new FilePropertiesMapModel(line);
        System.out.println("topicName: " + filePropertiesMapModel.getTopicName());
        System.out.println("mappropertis: " + filePropertiesMapModel.getMappropertis());

        System.out.println("........");


    }

}
