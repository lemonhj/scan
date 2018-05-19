package com.bigdata.datacenter.datasync.model.mongodb;

import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;

/**
 * Created by Dean on 2017/5/23.
 */
@Document(collection = "DS_STATUS")
public class SubjectStatus {
    private Long DS_ID;
    private String DS_NAME;
    private Long DS_TYPE;
    private Date DS_UPD_TIME;
    private Date DS_CRT_TIME;
    private Integer DS_STAS;
    private Date LAST_UPD_TIME;
    private Integer ERR_RUN_NUM;  //记录错误数据源的执行次数
}
