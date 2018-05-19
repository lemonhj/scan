package com.bigdata.datacenter.datasync.core.data;

import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.List;

/**
 * Created by Dean on 2017/5/22.
 */
public interface MongoTemplatePool {
    //根据数据源名字返回EbeanServer
    MongoTemplate getByName(String name);

    //获取所有的mongo数据源名称
    List<String> getDbBaseNames();

    //删除数据源
    void dropDbBase(String dbNm);
}
