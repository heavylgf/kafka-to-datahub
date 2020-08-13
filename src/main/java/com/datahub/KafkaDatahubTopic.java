package com.datahub;

import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.DatahubClientBuilder;
import com.aliyun.datahub.client.auth.AliyunAccount;
import com.aliyun.datahub.client.common.DatahubConfig;
import com.aliyun.datahub.client.exception.*;
import com.aliyun.datahub.client.http.HttpConfig;
import com.aliyun.datahub.client.model.ListTopicResult;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.RecordSchema;
import com.aliyun.datahub.client.model.TupleRecordData;
import com.util.DataHubPropertyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @Author: liugf
 * @Date: 2020/8/7 上午9:32
 */
public class KafkaDatahubTopic {

    private static final Logger Logger = LoggerFactory.getLogger(KafkaDatahubTopic.class);

    public static void main(String[] args) {
        DataHubPropertyUtils dataHubPropertyUtils = new DataHubPropertyUtils();
        String endpoint = dataHubPropertyUtils.getProperty("endpoint");
        String accessId = dataHubPropertyUtils.getProperty("accessId");
        String accessKey = dataHubPropertyUtils.getProperty("accessKey");
        String projectName = dataHubPropertyUtils.getProperty("projectName");
        String topicName = dataHubPropertyUtils.getProperty("topicName");

        Logger.info("projectName: " + projectName + ", topicName: " + topicName + ", Topic开始写入数据！！！！");

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

        tupleExampleTopic(datahubClient, projectName, topicName);
        Logger.info("projectName: " + projectName + ", topicName: " + topicName + ", Topic写入数据结束！！！！");

    }

    // 写入Tuple型数据
    public static void tupleExampleTopic(DatahubClient datahubClient, String projectName, String topicName) {
        String shardId = "0";

        Logger.info("获取Topic: " + topicName);

        // 获取schema
        RecordSchema recordSchema = datahubClient.getTopic(projectName, topicName).getRecordSchema();

        // 生成十条数据
        List<RecordEntry> recordEntries = new ArrayList<>();

        for (int i = 0; i < 10; ++i) {

            try{
                RecordEntry recordEntry = new RecordEntry();
                // 对每条数据设置额外属性，例如ip 机器名等。可以不设置额外属性，不影响数据写入
                TupleRecordData data = new TupleRecordData(recordSchema);
//                System.out.println("data" + data);

                BigDecimal id = new BigDecimal("2");
                data.setField("id", id);
                System.out.println("id: " + data.getField("id"));
                data.setField("data_whole_flag", "HelloWorld");
                System.out.println("data_whole_flag: " + data.getField("data_whole_flag"));
                data.setField("u1", new BigDecimal("11111111"));
                System.out.println("u1: " + data.getField("u1"));
                data.setField("u2", new BigDecimal("22222222"));
                System.out.println("u2: " + data.getField("u2"));
                data.setField("u3", new BigDecimal("33333333"));
                System.out.println("u3: " + data.getField("u3"));
                data.setField("u4", new BigDecimal("44444444"));

                data.setField("u5", BigDecimal.valueOf(555L));

                String time = "2019-07-01 10:00:00";
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                long timestamp = 0L;

                Date date = null;
                try {
                    date = simpleDateFormat.parse(time);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                timestamp = date.getTime();//获取时间的时间戳


                data.setField("time1", timestamp);

                recordEntry.setRecordData(data);
                recordEntry.setShardId(shardId);
                recordEntries.add(recordEntry);
                System.out.println("程序执行在这里！！！！！");

            }catch (Exception e){
                System.out.println("程序写入出错：" + e);
            }
        }

        try {
            // 服务端从2.12版本开始支持，之前版本请使用putRecords接口
            datahubClient.putRecordsByShard(projectName, topicName, shardId, recordEntries);
            // datahubClient.putRecords(projectName, topicName, recordEntries);
            System.out.println("write data successful");
        } catch (InvalidParameterException e) {
            System.out.println("invalid parameter, please check your parameter");
            System.exit(1);
        } catch (AuthorizationFailureException e) {
            System.out.println("AK error, please check your accessId and accessKey");
            System.exit(1);
        } catch (ResourceNotFoundException e) {
            System.out.println("project or topic or shard not found");
            System.exit(1);
        } catch (ShardSealedException e) {
            System.out.println("shard status is CLOSED, can not write");
            System.exit(1);
        } catch (DatahubClientException e) {
            System.out.println("other error");
            System.out.println(e);
            System.exit(1);
        }
    }

}
