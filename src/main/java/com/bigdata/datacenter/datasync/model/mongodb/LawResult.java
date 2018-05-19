package com.bigdata.datacenter.datasync.model.mongodb;

import org.bson.types.ObjectId;

import java.util.Date;

public class LawResult {
	private ObjectId idx;
	private Double ID; // ID
	private Date PUB_DT; // 发布日期
	private String TIT; // 标题
	private String MKT_NAME;// 市场名称
	private String COM_NAME;//发布机构
	private String PROV_DESC;// 设计省份
	private String INDU_NAME;// 设计省份
    private Double COM_ID;//公司ID
    private Double INDU_ID;//行业ID
    private String CONT;//内容
	private Date UPD_TIME; // 更新日期
	private Date ENT_TIME; // 结束日期
	private String TYP_NAME;//法律法规类别名称
	private String OPER_TYP_NAME;// 业务类型
	private String IS_VALID;// 是否有效
	private String INFO_SOUR;//来源
	
	public String getINFO_SOUR() {
		return INFO_SOUR;
	}
	
	public void setINFO_SOUR(String iNFO_SOUR) {
		INFO_SOUR = iNFO_SOUR;
	}
	
	public String getINDU_NAME() {
		return INDU_NAME;
	}
	public void setINDU_NAME(String iNDU_NAME) {
		INDU_NAME = iNDU_NAME;
	}
	public String getIS_VALID() {
		return IS_VALID;
	}
	public void setIS_VALID(String iS_VALID) {
		IS_VALID = iS_VALID;
	}
	public String getTYP_NAME() {
		return TYP_NAME;
	}
	public void setTYP_NAME(String tYP_NAME) {
		TYP_NAME = tYP_NAME;
	}
	public String getOPER_TYP_NAME() {
		return OPER_TYP_NAME;
	}
	public void setOPER_TYP_NAME(String oPER_TYP_NAME) {
		OPER_TYP_NAME = oPER_TYP_NAME;
	}
	public Double getID() {
		return ID;
	}
	public void setID(Double iD) {
		ID = iD;
	}
	public Date getPUB_DT() {
		return PUB_DT;
	}
	public void setPUB_DT(Date pUB_DT) {
		PUB_DT = pUB_DT;
	}
	public String getTIT() {
		return TIT;
	}
	public void setTIT(String tIT) {
		TIT = tIT;
	}
	public String getMKT_NAME() {
		return MKT_NAME;
	}
	public void setMKT_NAME(String mKT_NAME) {
		MKT_NAME = mKT_NAME;
	}
	public String getCOM_NAME() {
		return COM_NAME;
	}
	public void setCOM_NAME(String cOM_NAME) {
		COM_NAME = cOM_NAME;
	}
	public String getPROV_DESC() {
		return PROV_DESC;
	}
	public void setPROV_DESC(String pROV_DESC) {
		PROV_DESC = pROV_DESC;
	}
	public Double getCOM_ID() {
		return COM_ID;
	}
	public void setCOM_ID(Double cOM_ID) {
		COM_ID = cOM_ID;
	}
	public Double getINDU_ID() {
		return INDU_ID;
	}
	public void setINDU_ID(Double iNDU_ID) {
		INDU_ID = iNDU_ID;
	}
	public String getCONT() {
		return CONT;
	}
	public void setCONT(String cONT) {
		CONT = cONT;
	}
	public Date getUPD_TIME() {
		return UPD_TIME;
	}
	public void setUPD_TIME(Date uPD_TIME) {
		UPD_TIME = uPD_TIME;
	}
	public Date getENT_TIME() {
		return ENT_TIME;
	}
	public void setENT_TIME(Date eNT_TIME) {
		ENT_TIME = eNT_TIME;
	}
}
