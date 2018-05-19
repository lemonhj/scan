package com.bigdata.datacenter.datasync.model.mongodb;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.CompoundIndexes;
import org.springframework.data.mongodb.core.index.IndexDirection;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;
import java.util.List;

@Document(collection = "TXT_TIPS_LIST")
@CompoundIndexes({
		@CompoundIndex(name = "tips_bsi_super_idx", def = "{'BSI_TYP_CODEI': -1, 'TYP_SUPER': -1,'END_DT': -1}"),
		@CompoundIndex(name = "tips_bsi_big_idx", def = "{'BSI_TYP_CODEI': -1, 'BGN_DT': -1,'TYP_CODEII': -1}"),
		@CompoundIndex(name = "tips_bsi_pub_idx", def = "{'BSI_TYP_CODEI': -1, 'PUB_DT': -1,'TYP_CODEII': -1}"),
		@CompoundIndex(name = "tips_bsi_codeii_idx", def = "{'BSI_TYP_CODEI': -1, 'TYP_CODEII': -1}")
})
public class Tip {
	@Id
    private ObjectId idx;
	@Indexed(direction = IndexDirection.DESCENDING)
	private Double ID;
	@Indexed(direction = IndexDirection.DESCENDING)
	private Double	SECU_ID	; //证券编码     
	private Date	PUB_DT	; //发布日期     
	private Date	BGN_DT	; //开始日期
	@Indexed(direction = IndexDirection.DESCENDING)
	private Date	END_DT	; //截止日期     
	private String	NTC_CONT	; //提示内容     
	private Double	TYP_CODEI	; //提示大类     
	private Double	TYP_CODEII	; //提示细类     
	private String	TYP_NAMEI	; //提示类别名称1
	private String	TYP_NAMEII	; //提示类别名称2
	private Date	ENT_TIME	; //记录进表时间
	@Indexed(direction = IndexDirection.DESCENDING)
	private Date	UPD_TIME	; //记录修改时间 
	private String	RS_ID	; //来源标识     
	private String	TRD_CODE	; //证券代码
	private String	SECU_SHT	; //证券简称
	private Double	BSI_TYP_CODEI	; //证券大类
	private Double	LST_STS_CODE; // 上市状态
	private List<Integer> TYP_SUPER; // 大大类
	
	public Double getLST_STS_CODE() {
		return LST_STS_CODE;
	}
	public void setLST_STS_CODE(Double lST_STS_CODE) {
		LST_STS_CODE = lST_STS_CODE;
	}
	public String getTYP_NAMEI() {
		return TYP_NAMEI;
	}
	public void setTYP_NAMEI(String tYP_NAMEI) {
		TYP_NAMEI = tYP_NAMEI;
	}
	public List<Integer> getTYP_SUPER() {
		return TYP_SUPER;
	}
	public void setTYP_SUPER(List<Integer> tYP_SUPER) {
		TYP_SUPER = tYP_SUPER;
	}
	public Date getEND_DT() {
		return END_DT;
	}
	public void setEND_DT(Date eND_DT) {
		END_DT = eND_DT;
	}
	public ObjectId getIdx() {
		return idx;
	}
	public void setIdx(ObjectId idx) {
		this.idx = idx;
	}
	public Double getID() {
		return ID;
	}
	public void setID(Double iD) {
		ID = iD;
	}
	public Double getSECU_ID() {
		return SECU_ID;
	}
	public void setSECU_ID(Double sECU_ID) {
		SECU_ID = sECU_ID;
	}
	public Date getPUB_DT() {
		return PUB_DT;
	}
	public void setPUB_DT(Date pUB_DT) {
		PUB_DT = pUB_DT;
	}
	public Date getBGN_DT() {
		return BGN_DT;
	}
	public void setBGN_DT(Date bGN_DT) {
		BGN_DT = bGN_DT;
	}

	public String getNTC_CONT() {
		return NTC_CONT;
	}
	public void setNTC_CONT(String nTC_CONT) {
		NTC_CONT = nTC_CONT;
	}
	public Double getTYP_CODEI() {
		return TYP_CODEI;
	}
	public void setTYP_CODEI(Double tYP_CODEI) {
		TYP_CODEI = tYP_CODEI;
	}
	public Double getTYP_CODEII() {
		return TYP_CODEII;
	}
	public void setTYP_CODEII(Double tYP_CODEII) {
		TYP_CODEII = tYP_CODEII;
	}
	public String getTYP_NAMEII() {
		return TYP_NAMEII;
	}
	public void setTYP_NAMEII(String tYP_NAMEII) {
		TYP_NAMEII = tYP_NAMEII;
	}
	public Date getENT_TIME() {
		return ENT_TIME;
	}
	public void setENT_TIME(Date eNT_TIME) {
		ENT_TIME = eNT_TIME;
	}
	public Date getUPD_TIME() {
		return UPD_TIME;
	}
	public void setUPD_TIME(Date uPD_TIME) {
		UPD_TIME = uPD_TIME;
	}
	public String getRS_ID() {
		return RS_ID;
	}
	public void setRS_ID(String rS_ID) {
		RS_ID = rS_ID;
	}
	public String getTRD_CODE() {
		return TRD_CODE;
	}
	public void setTRD_CODE(String tRD_CODE) {
		TRD_CODE = tRD_CODE;
	}
	public String getSECU_SHT() {
		return SECU_SHT;
	}
	public void setSECU_SHT(String sECU_SHT) {
		SECU_SHT = sECU_SHT;
	}
	public Double getBSI_TYP_CODEI() {
		return BSI_TYP_CODEI;
	}
	public void setBSI_TYP_CODEI(Double bSI_TYP_CODEI) {
		BSI_TYP_CODEI = bSI_TYP_CODEI;
	}
}
