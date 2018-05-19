package com.bigdata.datacenter.datasync.model.mongodb;

import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;

/**
 * Created by Dean on 2017/5/23.
 */
@Document(collection = "TXT_RRP_STATUS")
public class TxtRrpStatus {
    private String DS_NAME;
    private Date DS_UPD_TIME;
    private Date DS_CRT_TIME;
    private Integer DS_STAS;
}
