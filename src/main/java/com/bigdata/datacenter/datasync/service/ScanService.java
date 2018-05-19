package com.bigdata.datacenter.datasync.service;

/**
 * Created by Dean on 2017/5/15.
 */
public interface ScanService {
    void incrementalSync();
    void totalSync();
}
