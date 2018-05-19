package com.bigdata.datacenter.datasync.service;

import com.bigdata.datacenter.datasync.model.mongodb.SubjectStatus;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Created by Dean on 2017/5/22.
 */
public interface SubjectStatusService {
    public static final int DS_STAS_NORMAL = 0;
    public static final int DS_STAS_UPDATE = 1;
    public static final int DS_STAS_ERROR = 2;

    List<SubjectStatus> findAll();

    SubjectStatus getSubStatusByDsName(String dsNm);

    int getDsStatus(String dsNm);

    void updateDsStatus(Map<String,Object> dds, int status,Date lastUpdTime);

    void resetDsStatus(int oldStatus,int status) ;

    void removeDsStatus(String dsName);
}
