package com.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.model.FilePropertiesMapModelUpdate;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author: liugf
 * @Date: 2020/8/11 下午5:29
 */
public class JsonTestUpdate {

    public static void main(String[] args) {

        // 读取topic 配置文件
        ReadFileParseMapUpdate readFileParse = new ReadFileParseMapUpdate();
        List<FilePropertiesMapModelUpdate> filePropertiesMapModelUpdateList = new ArrayList<>();
        try {
            filePropertiesMapModelUpdateList = readFileParse.readfile();
        } catch (Exception e) {
            System.out.println("加载topic 配置文件出错：" + e);
        }

        String json = "{\n" +
                "    \"sequence_num\" : \"1\", \n" +
                "    \"time_stamp\" : \"2020-08-11 15:35:42\", \n" +
                "    \"record_data\" : {\n" +
                "\t\"DATA_DATE\" : \"20200811000000\",\n" +
                "\t\"U89\" : \"\",\n" +
                "\t\"ID\" : \"6297524\",\n" +
                "\t\"DATA_POINT_FLAG\" : \"1\",\n" +
                "\t\"U11\" : \"239.800000000000000000\",\n" +
                "\t\"U10\" : \"239.100000000000000000\",\n" +
                "\t\"U13\" : \"238.100000000000000000\",\n" +
                "\t\"U12\" : \"241.300000000000000000\",\n" +
                "\t\"extend_field_1\" : \"050120200811204190003417127190129746 + 1\"\n" +
                "    }, \n" +
                "    \"op_type\" : \"I\", \n" +
                "    \"crc_code\" : \"52 b4b8d5\", \n" +
                "    \"table_name\" : \"E_MP_DAY_READ_HIGH\"\n" +
                "}";
        DateUtils dateUtils = new DateUtils();

//        JSONObject jsonObject = JSON.parseObject(json);

        // 遍历list: []
        for (int i = 0; i < filePropertiesMapModelUpdateList.size(); i++) {

            // Map<topic, HashMap<column, column_type>>
            for (Map.Entry<String, HashMap<String, String>> topic_colums :
                    filePropertiesMapModelUpdateList.get(i).getMapprtopics().entrySet()) {

                // 获取topicName
                String topicName = topic_colums.getKey();
                System.out.println("topicName: " + topicName);

                // 这里不是遍历，应该是通过key ，取出对应的value

                JSONObject jsonObject = JSON.parseObject(json);

                // 遍历当前topic的所有列
                for (Map.Entry<String, String> column : topic_colums.getValue().entrySet()) {
                    System.out.println("column: " + column.getKey() + ", column_type: " + column.getValue());
                    JSONObject json_record_data = JSON.parseObject(jsonObject.getString("record_data"));

                    if (json_record_data != null && json_record_data.getString(column.getKey()) != null) {
//                    String value = vo.getValue();
//                    System.out.println("vo.getValue: " + vo.getValue());  // decimal,string,
                        if (column.getValue().equals("decimal")) {
                            System.out.println("嵌套内：decimal_vo.getKey(): " + column.getKey());
                            System.out.println("嵌套内：decimal_vo.getValue(): " +
                                    new BigDecimal(json_record_data.getString(column.getKey())));
                        } else if (column.getValue().equals("timestamp")) {
                            System.out.println("嵌套内：timestamp_vo.getKey(): " + column.getKey());
                            System.out.println("嵌套内：timestamp_vo.getValue(): " +
                                    dateUtils.getTimestamp(json_record_data.getString(column.getKey())));
                        } else {
                            System.out.println("嵌套内：其余Key(): " + column.getKey());
                            System.out.println("嵌套内：其余Value(): " + json_record_data.getString(column.getKey()));
                        }
                        // 嵌套外的json字段不为空
                    } else if (jsonObject.getString(column.getKey()) != null) {
                        if (column.getValue().equals("decimal")) {
                            System.out.println("嵌套外：decimal_getKey(): " + column.getKey());
                            // 通过key 获取value
                            System.out.println("嵌套外：decimal_value: " + new BigDecimal(jsonObject.getString(column.getKey())));

                        } else if (column.getValue().equals("timestamp")) {
                            // 转换成时间时间戳
                            System.out.println("嵌套外：timestamp_vo_getKey(): " + column.getKey());
                            System.out.println("嵌套外：timestamp_vo.getValue(): " + dateUtils.getTimestamp(jsonObject.getString(column.getKey())));

//                        data.setField(vo.getKey(), dateUtils.getTimestamp(jsonObject.getString(vo.getKey())));
                        } else {
                            System.out.println("嵌套外：其余Key(): " + column.getKey());
                            System.out.println("嵌套外：其余Value(): " + jsonObject.getString(column.getKey()));

//                        data.setField(vo.getKey(), jsonObject.getString(vo.getKey()));
                        }

                    }
                }
            }
        }

//        String action = jsonObject.getString("action");
//        String id = jsonObject.getString("id");
//        System.out.println("action ="+action);//add
//        System.out.println("id ="+id);//1
//        System.out.println("jsonObject ="+jsonObject);
//        System.out.println("jsonObject_AddStatus ="+jsonObject.getJSONArray("AddStatus").size());
//        System.out.println("jsonObject_AddStatus[2] ="+jsonObject.getJSONArray("AddStatus").get(2));
//        System.out.println("jsonObject_AddStatus[2]_data ="+ JSON.parseObject(jsonObject.getJSONArray("AddStatus").get(2).toString()).get("Data"));
//        JSONObject json_data_data =  JSON.parseObject((jsonObject.getJSONArray("AddStatus").get(2).toString())).getJSONObject("Data").getJSONObject("Data");
//        System.out.println("jsonObject_AddStatus[2]_data_data ="+  json_data_data);
//        System.out.println("MaxRpm:" + json_data_data.getString("MaxRpm"));

    }

}
