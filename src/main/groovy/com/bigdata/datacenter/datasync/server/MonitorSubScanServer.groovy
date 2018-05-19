package com.bigdata.datacenter.datasync.server;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.bigdata.datacenter.datasync.service.impl.MonitorSubScanServiceImpl;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.bigdata.datacenter.datasync.utils.constants.MongoDBConfigConstants;
  
/**
 * 监控数据源扫描
 */                    
class MonitorSubScanServer {
    private static ClassPathXmlApplicationContext context;
                                                                                 
    public static void main(String[] args) {
        List<String> list = new ArrayList<String>(Arrays.asList(MongoDBConfigConstants.XML_FILES));
        list.add("quartz/applicationContext-Mointor.xml");
        context = new ClassPathXmlApplicationContext((String[])list.toArray());
    }         
}   
