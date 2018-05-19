package com.bigdata.datacenter.datasync.model.mongodb;

import org.springframework.data.mongodb.core.mapping.Document;

/**
 * Created by zhuchengxin on 2017/6/16.
 */
@Document(collection = "TXT_BLT_LIST_PROS")
public class BltNewPros {

    private String PROP_NAME;
    private String PROP_TYPE;

    public BltNewPros() {
    }

    public BltNewPros(String PROP_NAME, String PROP_TYPE) {
        this.PROP_NAME = PROP_NAME;
        this.PROP_TYPE = PROP_TYPE;
    }

    public String getPROP_NAME() {
        return PROP_NAME;
    }

    public void setPROP_NAME(String PROP_NAME) {
        this.PROP_NAME = PROP_NAME;
    }

    public String getPROP_TYPE() {
        return PROP_TYPE;
    }

    public void setPROP_TYPE(String PROP_TYPE) {
        this.PROP_TYPE = PROP_TYPE;
    }
}
