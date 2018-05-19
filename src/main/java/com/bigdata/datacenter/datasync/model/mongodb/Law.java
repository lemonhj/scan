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

@Document(collection = "TXT_LAWS_LIST")
@CompoundIndexes({
		@CompoundIndex(name = "lasws_pub_end_idx", def = "{'PUB_DT': -1, 'ENT_TIME': -1}")
})
public class Law {
	@Id
	private ObjectId idx;
	private Double ID; // ID
	private Date PUB_DT; // 发布日期
	private String TIT; // 标题
	private String MKT_NAME;// 市场名称
	private String COM_NAME;//发布机构
	private String PROV_DESC;// 设计省份
	@Indexed(direction = IndexDirection.DESCENDING)
	private List<Long> COM_ID	 = new ArrayList<Long>(); // 公司ID
	@Indexed(direction = IndexDirection.DESCENDING)
	private List<Long> INDU_ID	 = new ArrayList<Long>(); // 行业ID
    private String CONT;//内容
	private Date UPD_TIME; // 更新日期
	private Date ENT_TIME; // 结束日期
	private String COMBINE_SEARCH;//组合搜索
	private String INFO_SOUR;//来源
	
	public String getINFO_SOUR() {
		return INFO_SOUR;
	}
	public void setINFO_SOUR(String iNFO_SOUR) {
		INFO_SOUR = iNFO_SOUR;
	}
	public String getCOMBINE_SEARCH() {
		return COMBINE_SEARCH;
	}
	public void setCOMBINE_SEARCH(String cOMBINE_SEARCH) {
		COMBINE_SEARCH = cOMBINE_SEARCH;
	}
	public List<Long> getCOM_ID() {
		return COM_ID;
	}
	public void setCOM_ID(List<Long> cOM_ID) {
		COM_ID = cOM_ID;
	}
	public List<Long> getINDU_ID() {
		return INDU_ID;
	}
	public void setINDU_ID(List<Long> iNDU_ID) {
		INDU_ID = iNDU_ID;
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
	public ObjectId getIdx() {
		return idx;
	}
	public void setIdx(ObjectId idx) {
		this.idx = idx;
	}

	public static Law setValue(LawResult result, Law law) {
		law.setID(result.getID());
		law.setPUB_DT(result.getPUB_DT());
		law.setTIT(result.getTIT());
		law.setMKT_NAME(result.getMKT_NAME());
		law.setPROV_DESC(result.getPROV_DESC());
		law.setCONT(result.getCONT());
		law.setUPD_TIME(result.getUPD_TIME());
		law.setENT_TIME(result.getENT_TIME());
		law.setINFO_SOUR(result.getINFO_SOUR());
		String com_name = result.getCOM_NAME();
		// 判断COM_NAME是"其他"则用INFO_SOUR
		if (com_name == null || com_name.equals("其他")) {
			law.setCOM_NAME(result.getINFO_SOUR());
			com_name = result.getINFO_SOUR();
		} else {
			law.setCOM_NAME(com_name);
		}
		String searchStr = "";
		// 联合搜索字段
		if (result.getTYP_NAME() != null) {
			searchStr += result.getTYP_NAME();
		}
		if (result.getOPER_TYP_NAME() != null) {
			searchStr += "," + result.getOPER_TYP_NAME();
		}
		if (result.getMKT_NAME() != null) {
			searchStr += "," + result.getMKT_NAME();
		}
		if (com_name != null) {
			searchStr += "," + com_name;
		}
		if (result.getINDU_NAME() != null) {
			searchStr += "," + result.getINDU_NAME();
		}
		if (result.getPROV_DESC() != null) {
			searchStr += "," + result.getPROV_DESC();
		}
		if (!searchStr.equals("")) {
			if (searchStr.indexOf(",") == 0) {
				searchStr = searchStr.substring(1);
			}
			law.setCOMBINE_SEARCH(searchStr);
		}
		// 加入COM_ID
		if (result.getCOM_ID()!=null && !law.getCOM_ID().contains(result.getCOM_ID())) {
			law.getCOM_ID().add(result.getCOM_ID().longValue());
		}
		// 加入INDU_ID
		if (result.getINDU_ID()!=null && !law.getINDU_ID().contains(result.getINDU_ID())) {
			law.getINDU_ID().add(result.getINDU_ID().longValue());
		}
		return law;
	}
}
