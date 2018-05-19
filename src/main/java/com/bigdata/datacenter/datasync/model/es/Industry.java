package com.bigdata.datacenter.datasync.model.es;

import java.io.Serializable;

/***
 * 行业po
 * @author lizhiwei
 *
 */

public class Industry implements Serializable{
	 private String industry_code;
	 private String industry_name;
	public String getIndustry_code() {
		return industry_code;
	}
	public void setIndustry_code(String industry_code) {
		this.industry_code = industry_code;
	}
	public String getIndustry_name() {
		return industry_name;
	}
	public void setIndustry_name(String industry_name) {
		this.industry_name = industry_name;
	}
	 
	 
	 
}
