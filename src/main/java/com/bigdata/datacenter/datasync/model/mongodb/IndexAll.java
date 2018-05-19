package com.bigdata.datacenter.datasync.model.mongodb;

import java.util.Date;

public class IndexAll {
	private Double	CAT_CODE	; //指标分类代码
	private Double	DECI_NUM	; //指标展示小数位数
	private String	DS_ENG_NAME	; //数据源英文名称
	private String	DS_PARAM	; //数据源更新参数
	private String	EXT_SOUR	; //相同字段，不同数据源参数
	private String	FLD_NAME	; //指标显示名称
	private Double	ID	; //id
	private Double	IDX_ID	; //指标代码
	private String	IDX_NAME	; //指标名称
	private String	IDX_PARAM	; //指标参数
	private Double	IDX_TYP	; //指标类型
	private String	IS_USE	; //是否可用
	private Double	OBJ_ID	; //对象代码
	private Double	REF_IDX_CODE	; //指标引用代码
	private Double	SN	; //指标排序
	private Double	TYP_CODEI	; //证券分类代码，10股票，11基金，12债券
	private Double	UNIT_FCTR	; //单位系数
	private Double	UNIT_TYP_CODE	; //单位类型代码
	private String	UNIT_TYP_NAME	; //单位类型名称
	private Date	UPD_TIME	; //更新时间
	private String	VW_FMT	; //显示格式
	private Double	VW_UNIT_CODE	; //显示单位代码
	private String PARAM_DFT_VAL;
	private String SQL_CLAUSE;
	private String DS_PARAM_FULL;
	private Double DS_OBJ_ID; // 数据源objId
	private Double DS_TYP;
	private String UPD_PARAM;
	private String DEL_PARAM;
	private String UPD_KEY;
	private String INIT_SQL;
	
	public String getUPD_PARAM() {
		return UPD_PARAM;
	}
	public void setUPD_PARAM(String uPD_PARAM) {
		UPD_PARAM = uPD_PARAM;
	}
	public String getDEL_PARAM() {
		return DEL_PARAM;
	}
	public void setDEL_PARAM(String dEL_PARAM) {
		DEL_PARAM = dEL_PARAM;
	}
	public String getUPD_KEY() {
		return UPD_KEY;
	}
	public void setUPD_KEY(String uPD_KEY) {
		UPD_KEY = uPD_KEY;
	}
	public String getINIT_SQL() {
		return INIT_SQL;
	}
	public void setINIT_SQL(String iNIT_SQL) {
		INIT_SQL = iNIT_SQL;
	}
	public Double getDS_TYP() {
		return DS_TYP;
	}
	public void setDS_TYP(Double dS_TYP) {
		DS_TYP = dS_TYP;
	}
	public String getPARAM_DFT_VAL() {
		return PARAM_DFT_VAL;
	}
	public void setPARAM_DFT_VAL(String pARAM_DFT_VAL) {
		PARAM_DFT_VAL = pARAM_DFT_VAL;
	}
	public Double getDS_OBJ_ID() {
		return DS_OBJ_ID;
	}
	public void setDS_OBJ_ID(Double dS_OBJ_ID) {
		DS_OBJ_ID = dS_OBJ_ID;
	}
	public String getSQL_CLAUSE() {
		return SQL_CLAUSE;
	}
	public void setSQL_CLAUSE(String sQL_CLAUSE) {
		SQL_CLAUSE = sQL_CLAUSE;
	}
	public String getDS_PARAM_FULL() {
		return DS_PARAM_FULL;
	}
	public void setDS_PARAM_FULL(String dS_PARAM_FULL) {
		DS_PARAM_FULL = dS_PARAM_FULL;
	}
	public Double getCAT_CODE() {
		return CAT_CODE;
	}
	public void setCAT_CODE(Double cAT_CODE) {
		CAT_CODE = cAT_CODE;
	}
	public Double getDECI_NUM() {
		return DECI_NUM;
	}
	public void setDECI_NUM(Double dECI_NUM) {
		DECI_NUM = dECI_NUM;
	}
	public String getDS_ENG_NAME() {
		return DS_ENG_NAME;
	}
	public void setDS_ENG_NAME(String dS_ENG_NAME) {
		DS_ENG_NAME = dS_ENG_NAME;
	}
	public String getDS_PARAM() {
		return DS_PARAM;
	}
	public void setDS_PARAM(String dS_PARAM) {
		DS_PARAM = dS_PARAM;
	}
	public String getEXT_SOUR() {
		return EXT_SOUR;
	}
	public void setEXT_SOUR(String eXT_SOUR) {
		EXT_SOUR = eXT_SOUR;
	}
	public String getFLD_NAME() {
		return FLD_NAME;
	}
	public void setFLD_NAME(String fLD_NAME) {
		FLD_NAME = fLD_NAME;
	}
	public Double getID() {
		return ID;
	}
	public void setID(Double iD) {
		ID = iD;
	}
	public Double getIDX_ID() {
		return IDX_ID;
	}
	public void setIDX_ID(Double iDX_ID) {
		IDX_ID = iDX_ID;
	}
	public String getIDX_NAME() {
		return IDX_NAME;
	}
	public void setIDX_NAME(String iDX_NAME) {
		IDX_NAME = iDX_NAME;
	}
	public String getIDX_PARAM() {
		return IDX_PARAM;
	}
	public void setIDX_PARAM(String iDX_PARAM) {
		IDX_PARAM = iDX_PARAM;
	}
	public Double getIDX_TYP() {
		return IDX_TYP;
	}
	public void setIDX_TYP(Double iDX_TYP) {
		IDX_TYP = iDX_TYP;
	}
	public String getIS_USE() {
		return IS_USE;
	}
	public void setIS_USE(String iS_USE) {
		IS_USE = iS_USE;
	}
	public Double getOBJ_ID() {
		return OBJ_ID;
	}
	public void setOBJ_ID(Double oBJ_ID) {
		OBJ_ID = oBJ_ID;
	}
	public Double getREF_IDX_CODE() {
		return REF_IDX_CODE;
	}
	public void setREF_IDX_CODE(Double rEF_IDX_CODE) {
		REF_IDX_CODE = rEF_IDX_CODE;
	}
	public Double getSN() {
		return SN;
	}
	public void setSN(Double sN) {
		SN = sN;
	}
	public Double getTYP_CODEI() {
		return TYP_CODEI;
	}
	public void setTYP_CODEI(Double tYP_CODEI) {
		TYP_CODEI = tYP_CODEI;
	}
	public Double getUNIT_FCTR() {
		return UNIT_FCTR;
	}
	public void setUNIT_FCTR(Double uNIT_FCTR) {
		UNIT_FCTR = uNIT_FCTR;
	}
	public Double getUNIT_TYP_CODE() {
		return UNIT_TYP_CODE;
	}
	public void setUNIT_TYP_CODE(Double uNIT_TYP_CODE) {
		UNIT_TYP_CODE = uNIT_TYP_CODE;
	}
	public String getUNIT_TYP_NAME() {
		return UNIT_TYP_NAME;
	}
	public void setUNIT_TYP_NAME(String uNIT_TYP_NAME) {
		UNIT_TYP_NAME = uNIT_TYP_NAME;
	}
	public Date getUPD_TIME() {
		return UPD_TIME;
	}
	public void setUPD_TIME(Date uPD_TIME) {
		UPD_TIME = uPD_TIME;
	}
	public String getVW_FMT() {
		return VW_FMT;
	}
	public void setVW_FMT(String vW_FMT) {
		VW_FMT = vW_FMT;
	}
	public Double getVW_UNIT_CODE() {
		return VW_UNIT_CODE;
	}
	public void setVW_UNIT_CODE(Double vW_UNIT_CODE) {
		VW_UNIT_CODE = vW_UNIT_CODE;
	}
}
