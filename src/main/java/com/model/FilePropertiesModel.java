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
public class FilePropertiesModel {

    private static final Logger logger = LoggerFactory.getLogger(FilePropertiesModel.class);

    // topic 名称
    String topicName;
//    // 分隔符
//    String splitSign;
    // 列数
    int columnslength;
    // 所有列名称
    String[] columns;

    // 定义map
    Map<String, String> mappropertis;

    public FilePropertiesModel (String line) {
        // 通过逗号分隔
//        logger.info("通过逗号分隔每行.........");
        String [] allColumn = line.split(",");
//        logger.info("打印通过逗号分隔后的长度allColumn.length：" + allColumn.length);

        setTopicName(allColumn[0]);
//        logger.info("topic名称：" + topicName);

        setColumnslength(allColumn.length - 1);
//        logger.info("获取topic字段中的列数量: "+columnslength);

        // 初始化columns
        columns = new String[columnslength];
        String topicColumn = "";

        for (int i = 1; i < allColumn.length; i++) {
            // toLowerCase() 方法用于将大写字符转换为小写
            columns[i - 1] = allColumn[i];
//                    .toLowerCase();

            // 将topic列属性存入
//            topicColumn = topicColumn + columns[i - 1] + ",";

        }

//        logger.info("topic所有列信息：" + topicColumn.substring(0, topicColumn.length() - ",".length()));

    }


    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public int getColumnslength() {
        return columnslength;
    }

    public void setColumnslength(int columnslength) {
        this.columnslength = columnslength;
    }

    public String[] getColumns() {
        return columns;
    }

    public void setColumns(String[] columns) {
        this.columns = columns;
    }

}
