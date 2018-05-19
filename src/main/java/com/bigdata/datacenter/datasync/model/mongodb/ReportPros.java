package com.bigdata.datacenter.datasync.model.mongodb;

import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "RRP_RPT_BAS_PROS")
public class ReportPros {
    private String PROP_NAME;
	private String PROP_TYPE;
    public ReportPros(){}
    public ReportPros(String PROP_NAME, String PROP_TYPE){
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
