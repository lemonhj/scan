package com.bigdata.datacenter.datasync.service;

/**
 * Created by Dean on 2017/5/22.
 */
public interface TxtAndRrpStatusService {
    public static final int DS_STAS_NORMAL = 0;
    public static final int DS_STAS_UPDATE = 1;
    public static final int DS_STAS_ERROR = 2;

    void updateDsStatus(String dsNm, int status);
}
