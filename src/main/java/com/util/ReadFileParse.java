package com.util;

import com.model.FilePropertiesModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
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
public class ReadFileParse {

    static List<FilePropertiesModel> listpropertis = new ArrayList<FilePropertiesModel>() ;
//    static Map<String, String> mappropertis = new HashMap<String, String>();

    private static final Logger Logger = LoggerFactory.getLogger(ReadFileParse.class);

    public void readfile() throws Exception {
        Logger.info("开始加载topics.properties文件内容.......");
        InputStreamReader reader = new InputStreamReader(
                new FileInputStream("/development/eclipse/eclipse-workspace/kafka-to-datahub/src/main/resources/topics.properties"),
                "UTF-8");

        // 构造一个BufferedReader类来读取文件
        BufferedReader bufferedReader = new BufferedReader(reader);
//        String line = null;
        String line = bufferedReader.readLine();
        while (line != null) {
            FilePropertiesModel filePropertiesModel = new FilePropertiesModel(line);
            listpropertis.add(filePropertiesModel);
//            mappropertis.put(filePropertiesModel.getTopicName(), "1");
            line = bufferedReader.readLine();
//            Logger.info("配置大小: " + mappropertis.size());

        }

        for (int i = 0; i < listpropertis.size(); i++) {
            System.out.println("topicName: " + listpropertis.get(i).getTopicName());
            for(int j = 0; j < listpropertis.get(i).getColumns().length; j++){
                System.out.println("columns: " + listpropertis.get(i).getColumns()[j] );

            }
        }

        bufferedReader.close();
        Logger.info("加载topics.properties文件内容结束.......");

    }

    public static void main(String[] args) throws Exception {
        System.out.println("程序开始执行：.......");
        ReadFileParse readFileParse = new ReadFileParse();
        readFileParse.readfile();

    }

}
