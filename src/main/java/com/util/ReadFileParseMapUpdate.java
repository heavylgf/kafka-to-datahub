package com.util;

import com.model.FilePropertiesMapModelUpdate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 读取文件并解析
 * @Author: liugf
 * @Date: 2020/8/11 上午9:05
 */
public class ReadFileParseMapUpdate {

    static List<FilePropertiesMapModelUpdate> listTopicPropertis = new ArrayList<FilePropertiesMapModelUpdate>() ;

    private static final Logger Logger = LoggerFactory.getLogger(ReadFileParseMapUpdate.class);

    /**
     * 加载topic 配置文件
     * @return listTopicPropertis
     * @throws Exception
     */
    public List<FilePropertiesMapModelUpdate> readfile() throws Exception {
        Logger.info("开始加载topics.properties文件内容.......");
        InputStreamReader reader = new InputStreamReader(
                new FileInputStream("/development/eclipse/eclipse-workspace/kafka-to-datahub/src/main/resources/topicsMap.properties"),
                "UTF-8");

        // 构造一个BufferedReader类来读取文件
        BufferedReader bufferedReader = new BufferedReader(reader);
        String line = bufferedReader.readLine();
        while (line != null) {
            FilePropertiesMapModelUpdate filePropertiesMapModel = new FilePropertiesMapModelUpdate(line);
            // 存入 list 中
            listTopicPropertis.add(filePropertiesMapModel);
            line = bufferedReader.readLine();
//            Logger.info("执行在这里。。。。。。。。。 ");
        }

        bufferedReader.close();
        Logger.info("加载topics.properties文件内容结束.......");

        return listTopicPropertis;

//        for (int i = 0; i < listTopicPropertis.size(); i++) {
//            System.out.println("topicName: " + listTopicPropertis.get(i).getTopicName());
//
//            for (Map.Entry<String, String> vo : listTopicPropertis.get(i).getMappropertis().entrySet()) {
//                System.out.println("columns_key: " + vo.getKey() + ", columns_value: " + vo.getValue());
//
//            }
//        }

    }

    public static void main(String[] args) throws Exception {
        System.out.println("程序开始执行：.......");
//        ReadFileParseMap readFileParse = new ReadFileParseMap();
//        readFileParse.readfile();
        ReadFileParseMapUpdate readFileParse = new ReadFileParseMapUpdate();
        List<FilePropertiesMapModelUpdate> filePropertiesMapModelUpdateList = readFileParse.readfile();

        for (int i = 0; i < filePropertiesMapModelUpdateList.size(); i++) {
            filePropertiesMapModelUpdateList.get(i).getMapprtopics().entrySet();

            for (Map.Entry<String, HashMap<String, String>> vo :
                    filePropertiesMapModelUpdateList.get(i).getMapprtopics().entrySet()) {

                System.out.println("key: " + vo.getKey());
                System.out.println("value: " + vo.getValue());

                for (Map.Entry<String, String> column : vo.getValue().entrySet()) {
                    System.out.println("column: " + column.getKey() + ", column_type: " + column.getValue());
                }

            }


//            for (Map.Entry<String, String> vo : filePropertiesMapModelList.get(i)..entrySet()) {
//                System.out.println("columns_key: " + vo.getKey() + ", columns_value: " + vo.getValue());
//
//            }


        }


//        for (int i = 0; i < filePropertiesMapModelList.size(); i++) {
//
//            System.out.println("topicName: " + filePropertiesMapModelList.get(i).getTopicName());
//
//            for (Map.Entry<String, String> vo : filePropertiesMapModelList.get(i).getMappropertis().entrySet()) {
//                System.out.println("columns_key: " + vo.getKey() + ", columns_value: " + vo.getValue());
//
//            }
//
//        }

    }

}
