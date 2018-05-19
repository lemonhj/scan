package com.bigdata.datacenter.datasync.model.es;

import java.io.Serializable;

/****
 * 证券po
 * @author lizhiwei
 *
 */

public class Security implements Serializable{

	private static final long serialVersionUID = -8428984286517476680L;
	   private String secu_id;
	   private String secu_sht;
	   private String secu_name;
	   private String secu_sts;
	   private String secu_type;
	   private String bd_code;
	   
	public String getSecu_id() {
		return secu_id;
	}
	public void setSecu_id(String secu_id) {
		this.secu_id = secu_id;
	}

	public String getSecu_sht() {
		return secu_sht;
	}
	public void setSecu_sht(String secu_sht) {
		this.secu_sht = secu_sht;
	}

	public String getSecu_name() {
		return secu_name;
	}
	public void setSecu_name(String secu_name) {
		this.secu_name = secu_name;
	}
	public String getSecu_sts() {
		return secu_sts;
	}
	public void setSecu_sts(String secu_sts) {
		this.secu_sts = secu_sts;
	}

	public String getSecu_type() {
		return secu_type;
	}
	public void setSecu_type(String secu_type) {
		this.secu_type = secu_type;
	}
	public String getBd_code() {
		return bd_code;
	}
	public void setBd_code(String bd_code) {
		this.bd_code = bd_code;
	}

	   
}
