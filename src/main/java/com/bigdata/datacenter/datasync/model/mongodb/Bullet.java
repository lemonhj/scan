package com.bigdata.datacenter.datasync.model.mongodb;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.CompoundIndexes;
import org.springframework.data.mongodb.core.index.IndexDirection;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;

@Document(collection = "TXT_BLT_LIST")
@CompoundIndexes({
	@CompoundIndex(name = "city_region_idx", def = "{'PUB_DT': -1, 'ENT_TIME': -1}")
})
public class Bullet {
	@Id
    private String idx;
	@Indexed(direction = IndexDirection.DESCENDING)
	private Double	ID	; //ID
	private String	INFO_SOUR	; //信息来源
	private Date PUB_DT	; //发布日期
	@Indexed(direction = IndexDirection.DESCENDING)
	private Double	SECU_ID	; //证券id
	private String	SECU_SHT	; //证券简称
	private String	TIT	; //标题
	private String	TRD_CODE	; //交易代码
	@Indexed(direction = IndexDirection.DESCENDING)
	private Double	TYP_CODE	; //类型ID
	private String KEYW_NAME;
	private String CST_DESC; // 常量描述
	@Indexed(direction = IndexDirection.DESCENDING)
	private Double COM_ID;
	private Double TYP_CODEI; // 证券类别代码（大类）
	@Indexed(direction = IndexDirection.DESCENDING)
	private Date UPD_TIME; // 更新日期
	private Date ENT_TIME; // 结束日期
	private Double	SUB_TYP_CODE	; //类型ID
	private String	ANN_FMT; //文件格式
	private Double	FLD_CODE; //字段编号 
	private Double	FLD_VAL; //字段类型 
	private Double	ANN_ID; //附件ID
	private String CONT;//摘要
	
	public String getCONT() {
		return CONT;
	}

	public void setCONT(String cONT) {
		CONT = cONT;
	}

	public Double getANN_ID() {
		return ANN_ID;
	}

	public void setANN_ID(Double aNN_ID) {
		ANN_ID = aNN_ID;
	}

	public Double getFLD_CODE() {
		return FLD_CODE;
	}

	public void setFLD_CODE(Double fLD_CODE) {
		FLD_CODE = fLD_CODE;
	}

	public Double getFLD_VAL() {
		return FLD_VAL;
	}

	public void setFLD_VAL(Double fLD_VAL) {
		FLD_VAL = fLD_VAL;
	}

	public Double getSUB_TYP_CODE() {
		return SUB_TYP_CODE;
	}

	public void setSUB_TYP_CODE(Double sUB_TYP_CODE) {
		SUB_TYP_CODE = sUB_TYP_CODE;
	}

	public String getANN_FMT() {
		return ANN_FMT;
	}

	public void setANN_FMT(String aNN_FMT) {
		ANN_FMT = aNN_FMT;
	}

	public Double getTYP_CODEI() {
		return TYP_CODEI;
	}

	public void setTYP_CODEI(Double tYP_CODEI) {
		TYP_CODEI = tYP_CODEI;
	}

	public String getCST_DESC() {
		return CST_DESC;
	}

	public void setCST_DESC(String cST_DESC) {
		CST_DESC = cST_DESC;
	}

	public Double getCOM_ID() {
		return COM_ID;
	}

	public void setCOM_ID(Double cOM_ID) {
		COM_ID = cOM_ID;
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

	public String getIdx() {
		return idx;
	}

	public void setIdx(String idx) {
		this.idx = idx;
	}

	public String getKEYW_NAME() {
		return KEYW_NAME;
	}
	
	public void setKEYW_NAME(String kEYW_NAME) {
		KEYW_NAME = kEYW_NAME;
	}
	
	public Double getID() {
		return ID;
	}
	public void setID(Double iD) {
		ID = iD;
	}
	public String getINFO_SOUR() {
		return INFO_SOUR;
	}
	public void setINFO_SOUR(String iNFO_SOUR) {
		INFO_SOUR = iNFO_SOUR;
	}
	public Date getPUB_DT() {
		return PUB_DT;
	}
	public void setPUB_DT(Date pUB_DT) {
		PUB_DT = pUB_DT;
	}
	public Double getSECU_ID() {
		return SECU_ID;
	}
	public void setSECU_ID(Double sECU_ID) {
		SECU_ID = sECU_ID;
	}
	public String getSECU_SHT() {
		return SECU_SHT;
	}
	public void setSECU_SHT(String sECU_SHT) {
		SECU_SHT = sECU_SHT;
	}
	public String getTIT() {
		return TIT;
	}
	public void setTIT(String tIT) {
		TIT = tIT;
	}
	public String getTRD_CODE() {
		return TRD_CODE;
	}
	public void setTRD_CODE(String tRD_CODE) {
		TRD_CODE = tRD_CODE;
	}
	public Double getTYP_CODE() {
		return TYP_CODE;
	}
	public void setTYP_CODE(Double tYP_CODE) {
		TYP_CODE = tYP_CODE;
	}
}
