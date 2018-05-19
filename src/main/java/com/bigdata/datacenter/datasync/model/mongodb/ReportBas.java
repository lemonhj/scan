package com.bigdata.datacenter.datasync.model.mongodb;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.CompoundIndexes;
import org.springframework.data.mongodb.core.index.IndexDirection;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Document(collection = "RRP_RPT_BAS")
@CompoundIndexes({
		@CompoundIndex(name = "rrp_pub_end_idx", def = "{'PUB_DT': -1, 'ENT_TIME': -1}")
})
public class ReportBas {
	@Id
    private ObjectId idx;
	private String	ABST_SHT	; //文本摘要
	private Double	AREA_CODE	; //区域
	private String	AUT	; //撰写作者/研究员
	@Indexed(direction = IndexDirection.DESCENDING)
	private Double	COM_ID	; //撰写机构代码
	private String	COM_NAME	; //撰写机构名称
	private Double	EXCH_CODE	; //交易场所编码
	@Indexed(direction = IndexDirection.DESCENDING)
	private Double	ID	; //id
	private String	IS_WTR_MARK	; //是否有水印
	private String	KEYW	; //关键词
	private Double	LANG_TYP	; //语言类别
	private Double	OBJ_CODE	; //报告涉及对象
	private Date	PUB_DT	; //发布日期
	private Double	RPT_DEG	; //报告密级
	private Double	RPT_LVL	; //报告级别
	@Indexed(direction = IndexDirection.DESCENDING)
	private Double	RPT_TYP_CODE	; //报告类型
	private Double	SECT_CODE	; //主题板块编码
	private String	SUBJ_CODE	; //专题系列
	private String	SUB_TIT	; //副标题
	private String	TIT	; //标题
	private Date	WRT_DT	; //撰写日期
	@Indexed(direction = IndexDirection.DESCENDING)
	private Date UPD_TIME; // 更新日期
	private Date ENT_TIME; // 结束日期
	@Indexed(direction = IndexDirection.DESCENDING)
	private List<Long> INDU_ID	 = new ArrayList<Long>(); // 行业ID
	@Indexed(direction = IndexDirection.DESCENDING)
	private List<Long> SECU_ID	 = new ArrayList<Long>(); // 证券ID
	@Indexed(direction = IndexDirection.DESCENDING)
	private List<Long> FLD_VAL	 = new ArrayList<Long>(); // 研究员ID
    private String INDU_RAT_ORIG_DESC; // 原报告投资评级
    private String INDU_RAT_ORIG_DESC_LST; // 上次原报告投资评级
    private String RAT_ORIG_DESC; // 原报告投资评级
    private String RAT_ORIG_DESC_LST; //  上次原报告投资评级
    private Double TARG_PRC_MIN;  // 目标价格-最低(元)	
    private Double TARG_PRC_MAX;  // targ_prc_max
	private String CST_DESC; // 常量描述
	
	public String getCST_DESC() {
		return CST_DESC;
	}
	public void setCST_DESC(String cST_DESC) {
		CST_DESC = cST_DESC;
	}
	public String getINDU_RAT_ORIG_DESC() {
		return INDU_RAT_ORIG_DESC;
	}
	public void setINDU_RAT_ORIG_DESC(String iNDU_RAT_ORIG_DESC) {
		INDU_RAT_ORIG_DESC = iNDU_RAT_ORIG_DESC;
	}
	public String getINDU_RAT_ORIG_DESC_LST() {
		return INDU_RAT_ORIG_DESC_LST;
	}
	public void setINDU_RAT_ORIG_DESC_LST(String iNDU_RAT_ORIG_DESC_LST) {
		INDU_RAT_ORIG_DESC_LST = iNDU_RAT_ORIG_DESC_LST;
	}
	public String getRAT_ORIG_DESC() {
		return RAT_ORIG_DESC;
	}
	public void setRAT_ORIG_DESC(String rAT_ORIG_DESC) {
		RAT_ORIG_DESC = rAT_ORIG_DESC;
	}
	public String getRAT_ORIG_DESC_LST() {
		return RAT_ORIG_DESC_LST;
	}
	public void setRAT_ORIG_DESC_LST(String rAT_ORIG_DESC_LST) {
		RAT_ORIG_DESC_LST = rAT_ORIG_DESC_LST;
	}
	public Double getTARG_PRC_MIN() {
		return TARG_PRC_MIN;
	}
	public void setTARG_PRC_MIN(Double tARG_PRC_MIN) {
		TARG_PRC_MIN = tARG_PRC_MIN;
	}
	public Double getTARG_PRC_MAX() {
		return TARG_PRC_MAX;
	}
	public void setTARG_PRC_MAX(Double tARG_PRC_MAX) {
		TARG_PRC_MAX = tARG_PRC_MAX;
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
	public List<Long> getINDU_ID() {
		return INDU_ID;
	}
	public void setINDU_ID(List<Long> iNDU_ID) {
		INDU_ID = iNDU_ID;
	}
	public ObjectId getIdx() {
		return idx;
	}
	public void setIdx(ObjectId idx) {
		this.idx = idx;
	}

	public List<Long> getSECU_ID() {
		return SECU_ID;
	}
	public void setSECU_ID(List<Long> sECU_ID) {
		SECU_ID = sECU_ID;
	}
	public List<Long> getFLD_VAL() {
		return FLD_VAL;
	}
	public void setFLD_VAL(List<Long> fLD_VAL) {
		FLD_VAL = fLD_VAL;
	}
	public String getABST_SHT() {
		return ABST_SHT;
	}
	public void setABST_SHT(String aBST_SHT) {
		ABST_SHT = aBST_SHT;
	}
	public Double getAREA_CODE() {
		return AREA_CODE;
	}
	public void setAREA_CODE(Double aREA_CODE) {
		AREA_CODE = aREA_CODE;
	}
	public String getAUT() {
		return AUT;
	}
	public void setAUT(String aUT) {
		AUT = aUT;
	}
	public Double getCOM_ID() {
		return COM_ID;
	}
	public void setCOM_ID(Double cOM_ID) {
		COM_ID = cOM_ID;
	}
	public String getCOM_NAME() {
		return COM_NAME;
	}
	public void setCOM_NAME(String cOM_NAME) {
		COM_NAME = cOM_NAME;
	}
	public Double getEXCH_CODE() {
		return EXCH_CODE;
	}
	public void setEXCH_CODE(Double eXCH_CODE) {
		EXCH_CODE = eXCH_CODE;
	}
	public Double getID() {
		return ID;
	}
	public void setID(Double iD) {
		ID = iD;
	}
	public String getIS_WTR_MARK() {
		return IS_WTR_MARK;
	}
	public void setIS_WTR_MARK(String iS_WTR_MARK) {
		IS_WTR_MARK = iS_WTR_MARK;
	}
	public String getKEYW() {
		return KEYW;
	}
	public void setKEYW(String kEYW) {
		KEYW = kEYW;
	}
	public Double getLANG_TYP() {
		return LANG_TYP;
	}
	public void setLANG_TYP(Double lANG_TYP) {
		LANG_TYP = lANG_TYP;
	}
	public Double getOBJ_CODE() {
		return OBJ_CODE;
	}
	public void setOBJ_CODE(Double oBJ_CODE) {
		OBJ_CODE = oBJ_CODE;
	}
	public Date getPUB_DT() {
		return PUB_DT;
	}
	public void setPUB_DT(Date pUB_DT) {
		PUB_DT = pUB_DT;
	}
	public Double getRPT_DEG() {
		return RPT_DEG;
	}
	public void setRPT_DEG(Double rPT_DEG) {
		RPT_DEG = rPT_DEG;
	}
	public Double getRPT_LVL() {
		return RPT_LVL;
	}
	public void setRPT_LVL(Double rPT_LVL) {
		RPT_LVL = rPT_LVL;
	}
	public Double getRPT_TYP_CODE() {
		return RPT_TYP_CODE;
	}
	public void setRPT_TYP_CODE(Double rPT_TYP_CODE) {
		RPT_TYP_CODE = rPT_TYP_CODE;
	}
	public Double getSECT_CODE() {
		return SECT_CODE;
	}
	public void setSECT_CODE(Double sECT_CODE) {
		SECT_CODE = sECT_CODE;
	}
	public String getSUBJ_CODE() {
		return SUBJ_CODE;
	}
	public void setSUBJ_CODE(String sUBJ_CODE) {
		SUBJ_CODE = sUBJ_CODE;
	}
	public String getSUB_TIT() {
		return SUB_TIT;
	}
	public void setSUB_TIT(String sUB_TIT) {
		SUB_TIT = sUB_TIT;
	}
	public String getTIT() {
		return TIT;
	}
	public void setTIT(String tIT) {
		TIT = tIT;
	}
	public Date getWRT_DT() {
		return WRT_DT;
	}
	public void setWRT_DT(Date wRT_DT) {
		WRT_DT = wRT_DT;
	}
}
