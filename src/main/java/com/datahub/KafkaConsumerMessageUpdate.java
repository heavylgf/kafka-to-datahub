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
import com.model.FilePropertiesMapModelUpdate;
import com.util.DataHubPropertyUtils;
import com.util.DateUtils;
import com.util.ReadFileParseMap;
import com.util.ReadFileParseMapUpdate;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.util.*;

public class KafkaConsumerMessageUpdate {
    private static final Logger Logger = LoggerFactory.getLogger(KafkaConsumerMessageUpdate.class);

    private static KafkaConsumer consumer;
    private static Properties properties;

    static {
        // 配置kafka属性值
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
    private void consumerMessageAutoCommit(List<FilePropertiesMapModelUpdate> filePropertiesMapModelList,
                                           String projectName, DatahubClient datahubClient) {
        int NUMBER = 1000;
        // 创建消费者
        consumer = new KafkaConsumer(properties);
        DateUtils dateUtils = new DateUtils();
        String shardId = "0";
        // 订阅消费者topic
        for (int i = 0; i < filePropertiesMapModelList.size(); i++) {

            for (Map.Entry<String, HashMap<String, String>> topic_colums :
                    filePropertiesMapModelList.get(i).getMapprtopics().entrySet()) {

                // 获取topicName
                String topicName = topic_colums.getKey();
                // 订阅消费者topic
                consumer.subscribe(Arrays.asList(topicName));

//                System.out.println("key: " + topic_colums.getKey());
//                System.out.println("value: " + topic_colums.getValue());

                // 获取schema
                RecordSchema recordSchema = datahubClient.getTopic(projectName, topicName).getRecordSchema();

                List<RecordEntry> recordEntries = new ArrayList<>();
                // 获取数据
                ConsumerRecords<String, String> records = consumer.poll(NUMBER);

                while(true){
                    if (records != null && records.count() > 0) {
                        // 遍历每条数据 json
                        for(ConsumerRecord<String, String> record : records) {
                            try {

//                        System.out.println(record.timestamp() + "," + record.topic() + ","
//                                + record.partition() + "," + record.offset() + " "
//                                + record.key() + "," + record.value());

                                RecordEntry recordEntry = new RecordEntry();
                                TupleRecordData data = new TupleRecordData(recordSchema);

                                // 获取json中的 value
                                String kafkaMessages = record.value();

                                // 解析json
                                JSONObject jsonObject = JSON.parseObject(kafkaMessages);

                                // 通过获取该topic字段类型，来赋值给data
                                for (Map.Entry<String, String> column : topic_colums.getValue().entrySet()) {
                                    System.out.println("column: " + column.getKey() + ", column_type: " + column.getValue());
                                    // column: DATA_POINT_FLAG, column_type: decimal

                                    // 获取topic中key对应的value不为null的字段
                                    if (jsonObject.getString(column.getKey()) != null) {
                                        if (column.getValue().equals("decimal")) {
//                                            String key1 = vo.getKey();
//                                            String value1 = jsonObject.getString(vo.getKey());
                                            data.setField(column.getKey(), new BigDecimal(jsonObject.getString(column.getKey())));
                                        } else if (column.getValue().equals("timestamp")) {
                                            // 转换成时间时间戳
                                            data.setField(column.getKey(), dateUtils.getTimestamp(jsonObject.getString(column.getKey())));
                                        } else {
                                            data.setField(column.getKey(), jsonObject.getString(column.getKey()));
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
        ReadFileParseMapUpdate readFileParse = new ReadFileParseMapUpdate();
        List<FilePropertiesMapModelUpdate> filePropertiesMapModelList = new ArrayList<>();
        try {
            filePropertiesMapModelList = readFileParse.readfile();
        } catch (Exception e) {
            System.out.println("加载topic 配置文件出错：" + e);

        }

        KafkaConsumerMessageUpdate kafkaConsumerMessage = new KafkaConsumerMessageUpdate();
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
