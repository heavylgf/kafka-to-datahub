package com.datahub;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.DatahubClientBuilder;
import com.aliyun.datahub.client.auth.AliyunAccount;
import com.aliyun.datahub.client.common.DatahubConfig;
import com.aliyun.datahub.client.exception.*;
import com.aliyun.datahub.client.http.HttpConfig;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.RecordSchema;
import com.aliyun.datahub.client.model.TupleRecordData;
import com.model.FilePropertiesMapModel;
import com.util.DataHubPropertyUtils;
import com.util.DateUtils;
import com.util.ReadFileParseMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.*;

public class KafkaConsumerMessage {
    private static final Logger Logger = LoggerFactory.getLogger(KafkaConsumerMessage.class);

    private static KafkaConsumer consumer;
    private static Properties properties;

    static {
        // 配置属性值
        properties = new Properties();
        DataHubPropertyUtils propertyUtils = new DataHubPropertyUtils();
        // kafka是服务器地址
        properties.put("bootstrap.servers", propertyUtils.getProperty("bootstrap.servers"));
        properties.put("group.id", propertyUtils.getProperty("groupId"));
        // 自动提交
        properties.put("enable.auto.commit", propertyUtils.getProperty("enable.auto.commit"));
        // 自动处理的间隔时间1秒
        properties.put("auto.commit.interval.ms", propertyUtils.getProperty("auto.commit.interval.ms"));
        properties.put("session.timeout.ms", propertyUtils.getProperty("session.timeout.ms"));
        properties.put("auto.offset.reset", propertyUtils.getProperty("auto.offset.reset"));
        //key和values的持久化设置
        properties.put("key.deserializer", propertyUtils.getProperty("key.deserializer"));
        properties.put("value.deserializer", propertyUtils.getProperty("value.deserializer"));

    }

    /**
     * consumer自动提交
     * @param filePropertiesMapModelList
     * @param projectName
     * @param datahubClient
     */
    private void consumerMessageAutoCommit(List<FilePropertiesMapModel> filePropertiesMapModelList,
                                                         String projectName, DatahubClient datahubClient) {
        int NUMBER = 10;
        // 创建消费者
        consumer = new KafkaConsumer(properties);
        DateUtils dateUtils = new DateUtils();
        String shardId = "0";
        // 订阅消费者topic
        for (int i = 0; i < filePropertiesMapModelList.size(); i++) {
            // 获取topicName
            String kafkaTopicName = filePropertiesMapModelList.get(i).getKafkaTopicName();
            String datahubTopicName = filePropertiesMapModelList.get(i).getDatahubTopicName();
            consumer.subscribe(Arrays.asList(kafkaTopicName));

//            Logger.info("bootstrap.servers：" + properties.getProperty("bootstrap.servers") + " ，" +
//                    "group.id：" + properties.getProperty("groupId") + "，" +
//                    "topic：" + topicName);

            // 获取datahub中的 schema
            RecordSchema recordSchema = datahubClient.getTopic(projectName, datahubTopicName).getRecordSchema();
            List<RecordEntry> recordEntries = new ArrayList<>();
            // 一次性获取kafka中100条数据
            ConsumerRecords<String, String> records = consumer.poll(NUMBER);
            while(true){
                if (records != null && records.count() > 0) {
                    // 遍历kafka中的每条数据
                    for(ConsumerRecord<String, String> record : records) {
                        try {
                            RecordEntry recordEntry = new RecordEntry();
                            TupleRecordData data = new TupleRecordData(recordSchema);

                            // 获取值value，拿到的是一条json数据，并进行解析
                            String kafkaMessages = record.value();
                            JSONObject jsonObject = JSON.parseObject(kafkaMessages);

                            // 通过获取该topic字段类型，来赋值给data
                            for (Map.Entry<String, String> column_types : filePropertiesMapModelList.get(i).getMappropertis().entrySet()) {

                                // 获取字段名
                                String column = column_types.getKey();
                                // 获取字段类型
                                String type = column_types.getValue();

                                // 根据列名查询kafka中解析的数据是否有值
                                if (jsonObject.getString(column) != null) {
                                    // 判断字段类型
                                    if (type.equals("DECIMAL")) {
                                        // 将json解析的值写入datahub中
                                        data.setField(column, new BigDecimal(jsonObject.getString(column)));
                                    } else if (type.equals("TIMESTAMP") ) {
                                        // 转换成时间时间戳
                                        data.setField(column, dateUtils.getTimestamp(jsonObject.getString(column)));
                                    } else {
                                        data.setField(column, jsonObject.getString(column));
                                    }
                                }
                            }

                            recordEntry.setRecordData(data);
                            recordEntry.setShardId(shardId);
                            recordEntries.add(recordEntry);

                        } catch (Exception e) {
                            System.out.println("程序写入出错：" + e);
                        }
                    }

                    try {
                        // 服务端从2.12版本开始支持，之前版本请使用putRecords接口
                        datahubClient.putRecordsByShard(projectName, datahubTopicName, shardId, recordEntries);
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
        }
    }

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

        // 读取topic 配置文件
        ReadFileParseMap readFileParseMap = new ReadFileParseMap();
        List<FilePropertiesMapModel> filePropertiesMapModelList = new ArrayList<>();
        try {
            filePropertiesMapModelList = readFileParseMap.readfile();
        } catch (Exception e) {
            System.out.println("加载topic 配置文件出错：" + e);

        }

        KafkaConsumerMessage kafkaConsumerMessage = new KafkaConsumerMessage();
        kafkaConsumerMessage.consumerMessageAutoCommit(filePropertiesMapModelList, projectName, datahubClient);

    }


//    KafkaConsumerMessage kafkaConsumerMessage = new KafkaConsumerMessage();
//
//    @Override
//    public void run() {
//
//        try {
//            while (true) {
//                kafkaConsumerMessage.messageHandle(consumer, records);
//            }
//        }catch (Exception e){
//            e.printStackTrace();
//        }finally {
//            consumer.close();
//        }
//
//    }

}
