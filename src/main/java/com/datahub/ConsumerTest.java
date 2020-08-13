package com.datahub;

import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.DatahubClientBuilder;
import com.aliyun.datahub.client.auth.AliyunAccount;
import com.aliyun.datahub.client.common.DatahubConfig;
import com.aliyun.datahub.client.http.HttpConfig;
import com.model.FilePropertiesMapModel;
import com.util.DataHubPropertyUtils;
import com.util.ReadFileParseMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: liugf
 * @Date: 2020/8/13 下午12:18
 */
public class ConsumerTest {

    private static final Logger Logger = LoggerFactory.getLogger(ConsumerTest.class);

    public static void main(String[] args) {

        // 获取dataHub配置信息
        DataHubPropertyUtils dataHubPropertyUtils = new DataHubPropertyUtils();
        String endpoint = dataHubPropertyUtils.getProperty("endpoint");
        String accessId = dataHubPropertyUtils.getProperty("accessId");
        String accessKey = dataHubPropertyUtils.getProperty("accessKey");
        String projectName = dataHubPropertyUtils.getProperty("projectName");
//        String topicName = dataHubPropertyUtils.getProperty("topicName");

        Logger.info("projectName: " + projectName + ", Topic开始写入数据！！！！");

        // 创建DataHubClient实例
        DatahubClient datahubClient = DatahubClientBuilder.newBuilder()
                .setDatahubConfig(
                        new DatahubConfig(endpoint,
                                // 是否开启二进制传输，服务端2.12版本开始支持
                                new AliyunAccount(accessId, accessKey), true))
                //专有云使用出错尝试将参数设置为           false
                // HttpConfig可不设置，不设置时采用默认值
                .setHttpConfig(new HttpConfig()
                        .setCompressType(HttpConfig.CompressType.LZ4) // 读写数据推荐打开网络传输 LZ4压缩
                        .setConnTimeout(10000))
                .build();

        // 读取dataHub中Topic 配置文件
        ReadFileParseMap readFileParseMap = new ReadFileParseMap();
        List<FilePropertiesMapModel> filePropertiesMapModelList = new ArrayList<>();
        try {
            // 加载配置文件到list中
            filePropertiesMapModelList = readFileParseMap.readfile();
        } catch (Exception e) {
            System.out.println("加载topic 配置文件出错：" + e);

        }

        int consumerNum = 10;
        List<ConsumerRunnable> consumers = new ArrayList<>(consumerNum);;
        for (int i = 0; i < consumerNum; i++) {
            ConsumerRunnable consumerThread =
                    new ConsumerRunnable(filePropertiesMapModelList, projectName, datahubClient);
            consumers.add(consumerThread);
        }

        for (ConsumerRunnable task : consumers) {
            new Thread(task).start();
        }

//        for (int i = 0; i < consumerNum; ++i) {
//            new Thread(consumerThread).start();
//        }

    }

}
