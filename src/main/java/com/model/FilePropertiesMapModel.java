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

    // kafka topic 名称
    private String kafkaTopicName;
    // datahub topic 名称
    private String datahubTopicName;

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
        setKafkaTopicName(allColumn[0]);
        setDatahubTopicName(allColumn[1]);

        mappropertis = new HashMap<String, String>();
        for (int i = 2; i < allColumn.length; i++) {
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

    public String getKafkaTopicName() {
        return kafkaTopicName;
    }

    public void setKafkaTopicName(String kafkaTopicName) {
        this.kafkaTopicName = kafkaTopicName;
    }

    public String getDatahubTopicName() {
        return datahubTopicName;
    }

    public void setDatahubTopicName(String datahubTopicName) {
        this.datahubTopicName = datahubTopicName;
    }

    public HashMap<String, String> getMappropertis() {
        return mappropertis;
    }

    public void setMappropertis(HashMap<String, String> mappropertis) {
        this.mappropertis = mappropertis;
    }

    public static void main(String[] args) {

        String line = "tp_amr_p_e_data_mp_his_prd,tp_amr_p_hbase_e_data_mp_his_prd,T_FACTOR:DECIMAL,SELF_FACTOR:DECIMAL," +
                "ASSET_NO:STRING,TMNL_ASSET_NO:STRING,TERMINAL_ADDR:STRING,CONS_SORT:STRING,CONS_NO:STRING," +
                "CIS_TMNL_ASSET_NO:STRING,COMM_ADDR:STRING,DISABLE_DATE:TIMESTAMP,DATA_SRC:STRING,ID:DECIMAL," +
                "IS_VALID:DECIMAL,AREA_CODE:STRING,AREA_NO:STRING,ORG_NO:STRING,ERATE_OFFSET:DECIMAL,IS_FQD:STRING," +
                "PT:DECIMAL,MET_PASSWORD:STRING,METER_ID:DECIMAL,CT:DECIMAL,CREATE_DATE:TIMESTAMP,MP_SN_NAME:STRING," +
                "MP_TYPE:DECIMAL,MP_SN:DECIMAL,CALC_MODE:DECIMAL,PQ_FLAG:STRING,COLL_OBJ_ID:DECIMAL,FC_GC_FLAG:DECIMAL," +
                "EXTEND_FIELD_ORG_CODE:STRING,EXTEND_FIELD_SRC_SYSTEM:STRING,EXTEND_FIELD_UPDATE_FLAG:STRING," +
                "EXTEND_FIELD_UPDATE_TIME:TIMESTAMP,EXTEND_FIELD_VALID_FLAG:STRING,EXTEND_FIELD_TIME_STAMP:TIMESTAMP," +
                "EXTEND_FIELD_1:STRING,EXTEND_FIELD_2:STRING,EXTEND_FIELD_3:STRING,EXTEND_FIELD_4:STRING," +
                "EXTEND_FIELD_5:STRING,EXTEND_FIELD_6:STRING,EXTEND_FIELD_7:STRING,EXTEND_FIELD_8:STRING," +
                "EXTEND_FIELD_9:TIMESTAMP,EXTEND_FIELD_10:TIMESTAMP";
        FilePropertiesMapModel filePropertiesMapModel = new FilePropertiesMapModel(line);
        System.out.println("kafkaTopicName: " + filePropertiesMapModel.getKafkaTopicName());
        System.out.println("datahubTopicName: " + filePropertiesMapModel.getDatahubTopicName());

        for (Map.Entry<String, String> column_type : filePropertiesMapModel.getMappropertis().entrySet()) {
            System.out.println("column: " + column_type.getKey() + ", type: " + column_type.getValue());
            System.out.println("---------------------");

        }

//        System.out.println("mappropertis: " + filePropertiesMapModel.getMappropertis());

    }

}
