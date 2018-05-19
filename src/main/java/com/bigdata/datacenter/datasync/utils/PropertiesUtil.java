package com.bigdata.datacenter.datasync.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


/**
 *
 * PropertiesUtil.java
 *
 * @desc properties 资源文件解析工具
 * @author Guoxp
 * @datatime Apr 7, 2013 3:58:45 PM
 *
 */
public class PropertiesUtil {

    private static Properties props;
    private static URI uri;
    private static String FILE_NAME = "application.properties";
    public static String LAST_SCAN_TIME_FILE = "scan.properties";
    public final static String LAST_SCAN_TIME = "idx.lastScanTime";
    public final static String LAST_SCAN_IDX = "lastScanIdx";
    // 属性变更时间变量
	public static final String LAST_IDX_CHG_DATE = "lastIdxChgDate";

    private static void init() {
        try {
            props = new Properties();
            InputStream fis =PropertiesUtil.class.getClassLoader().getResourceAsStream(FILE_NAME);
            props.load(fis);
            uri = PropertiesUtil.class.getClassLoader().getResource(FILE_NAME).toURI();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    /**
     * 获取某个属性
     */
    public static String getProperty(String key){
        init();
        return props.getProperty(key);
    }

    /**
     * 获取某个属性
     */
    public static String getProperty(String filePath,String key){
        try {
            Properties properties = new Properties();
            InputStream fis = PropertiesUtil.class.getClassLoader().getResourceAsStream(filePath);
            properties.load(fis);
            return properties.getProperty(key);
        }catch (Exception err){
            err.printStackTrace();
        }
        return "";
    }
    /**
     * 获取所有属性，返回一个map,不常用
     * 可以试试props.putAll(t)
     */
    public Map getAllProperty(){
        Map map=new HashMap();
        Enumeration enu = props.propertyNames();
        while (enu.hasMoreElements()) {
            String key = (String) enu.nextElement();
            String value = props.getProperty(key);
            map.put(key, value);
        }
        return map;
    }
    /**
     * 在控制台上打印出所有属性，调试时用。
     */
    public void printProperties(){
        props.list(System.out);
    }
    /**
     * 写入properties信息
     */
    public static void writeProperties(String key, String value) {
        OutputStream fos = null;
        try {
            init();
            fos = new FileOutputStream(new File(uri));
            props.setProperty(key, value);
            // 将此 Properties 表中的属性列表（键和元素对）写入输出流
            props.store(fos, "『comments』Update key：" + key);
        } catch (Exception e) {
            e.printStackTrace();
        }finally{
            if(fos != null){
                try {
                    fos.close();
                }catch (Exception err){}
            }
        }
    }

    /**
     * 写入properties信息
     */
    public static void writeProperties(String filePath,String key, String value) {
        OutputStream fos = null;
        try {
            Properties properties = new Properties();
            InputStream fis = PropertiesUtil.class.getClassLoader().getResourceAsStream(filePath);
            properties.load(fis);
            URI writeUrl = PropertiesUtil.class.getClassLoader().getResource(filePath).toURI();
            fos = new FileOutputStream(new File(writeUrl));
            properties.setProperty(key, value);
            // 将此 Properties 表中的属性列表（键和元素对）写入输出流
            properties.store(fos, "『comments』Update key：" + key);
        } catch (Exception e) {
            e.printStackTrace();
        }finally{
            if(fos != null){
                try {
                    fos.close();
                }catch (Exception err){}
            }
        }
    }

    public static void main(String[] args) {
        PropertiesUtil prop = new PropertiesUtil();
        prop.init();

        String result =  prop.getProperty("111");
        System.out.println(result);
    }
}