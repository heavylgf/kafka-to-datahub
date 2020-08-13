package com.util;

import com.model.FilePropertiesMapModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.*;

/**
 * 读取文件并解析
 * @Author: liugf
 * @Date: 2020/8/11 上午9:05
 */
public class ReadFileParseMap {

    static List<FilePropertiesMapModel> listTopicPropertis = new ArrayList<FilePropertiesMapModel>() ;

    private static final Logger Logger = LoggerFactory.getLogger(ReadFileParseMap.class);

    /**
     * 加载topic 配置文件
     * @return listTopicPropertis
     * @throws Exception
     */
    public List<FilePropertiesMapModel> readfile() throws Exception {
        Logger.info("开始加载topicsmapcolumn.properties文件内容.......");
        InputStreamReader reader = new InputStreamReader(
                new FileInputStream("/development/eclipse/eclipse-workspace/kafka-to-datahub/src/main/resources/topicsmapcolumn.properties"),
                "UTF-8");

        // 构造一个BufferedReader类来读取文件
        BufferedReader bufferedReader = new BufferedReader(reader);
        String line = bufferedReader.readLine();
        while (line != null) {
            FilePropertiesMapModel filePropertiesMapModel = new FilePropertiesMapModel(line);
            // 存入 list 中
            listTopicPropertis.add(filePropertiesMapModel);
            line = bufferedReader.readLine();
//            Logger.info("执行在这里。。。。。。。。。 ");
        }

        bufferedReader.close();
        Logger.info("加载topicsmapcolumn.properties文件内容结束.......");

        return listTopicPropertis;
    }


    public static void main(String[] args) throws Exception {
        System.out.println("程序开始执行：.......");
        ReadFileParseMap readFileParseMap = new ReadFileParseMap();
        List<FilePropertiesMapModel> filePropertiesMapModelList = readFileParseMap.readfile();

        // size: 130
        System.out.println("size: " + filePropertiesMapModelList.size());

        for (int i = 0; i < filePropertiesMapModelList.size(); i++) {
            System.out.println("kafkaTopicName: " + filePropertiesMapModelList.get(i).getKafkaTopicName());
            System.out.println("datahubTopicName: " + filePropertiesMapModelList.get(i).getDatahubTopicName());

//            for (Map.Entry<String, String> column : filePropertiesMapModelList.get(i).getMappropertis().entrySet()) {
//                System.out.println("column: " + column.getKey() + ", column_type: " + column.getValue());
//            }
        }

    }

}
