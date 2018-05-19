package com.bigdata.datacenter.datasync.core.data;

import com.avaje.ebean.EbeanServer;

/**
 * Created by happy on 2015/11/24.
 */
public interface EbeanServerPool {
    //根据数据源名字返回EbeanServer
    EbeanServer getByName(String name);
}
