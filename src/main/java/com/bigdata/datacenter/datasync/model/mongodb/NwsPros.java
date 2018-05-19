package com.bigdata.datacenter.datasync.model.mongodb;

public class NwsPros {
    private String PROP_NAME;
	private String PROP_TYPE;
    public NwsPros(){}
    public NwsPros(String PROP_NAME, String PROP_TYPE){
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
