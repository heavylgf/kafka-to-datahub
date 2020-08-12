package com.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * properties配置文件获取工具类
 */
public class DataHubPropertyUtils {

    private static final Logger Logger = LoggerFactory.getLogger(DataHubPropertyUtils.class);
    private static Properties props;
    static{
        loadProps();
    }

    synchronized static private void loadProps(){
        Logger.info("开始加载datahub.properties文件内容.......");
        props = new Properties();
        InputStream in = null;
        try {
            // 第一种，通过类加载器进行获取properties文件流
            in = DataHubPropertyUtils.class.getClassLoader().getResourceAsStream("datahub.properties");
            // 第二种，通过类进行获取properties文件流
            // in = PropertyUtil.class.getResourceAsStream("/jdbc.properties");
            props.load(in);
        } catch (FileNotFoundException e) {
            Logger.error("properties.properties文件未找到");
        } catch (IOException e) {
            Logger.error("出现IOException");
        } finally {
            try {
                if(null != in) {
                    in.close();
                }
            } catch (IOException e) {
                Logger.error("properties.properties文件流关闭出现异常");
            }
        }

//        Logger.info("加载properties文件内容完成...........");
//        Logger.info("properties文件内容：" + props);

    }

    public static String getProperty(String key){
        if(null == props) {
            loadProps();
        }
        return props.getProperty(key);
    }

    public static String getProperty(String key, String defaultValue) {
        if(null == props) {
            loadProps();
        }
        return props.getProperty(key, defaultValue);
    }

}
