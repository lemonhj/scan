package com.bigdata.datacenter.datasync.service.impl

import com.avaje.ebean.SqlRow
import com.avaje.ebean.Update
import com.bigdata.datacenter.datasync.utils.DateUtils
import com.bigdata.datacenter.datasync.utils.EntityUtil
import com.bigdata.datacenter.datasync.utils.constants.MongoDBConfigConstants
import com.bigdata.datacenter.metadata.service.ResultHandler
import org.apache.log4j.Logger
import org.springframework.data.mongodb.core.MongoOperations
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query

import java.rmi.MarshalledObject
import java.sql.Timestamp

/**
 * Created by Dan on 2017/7/12.
 */
class AllIndexMacroDsResultHandler implements ResultHandler{

    private static final Logger logger = Logger.getLogger(AllIndexMacroDsResultHandler.class)
    private MongoOperations idMarcoTemplate
    private List props
    private String dsNm
    public Date maxUpdTime
    private static final String MACRO_IDX_ID = "IDX_ID";
	private static final String MACRO_END_DT = "ENDDATE";
	private static final String MACRO_IDX_VAL = "IDX_VAL";
	private static final String MACRO_UPD_TIME = "UPD_TIME";

    public AllIndexMacroDsResultHandler (MongoOperations idMarcoTemplate, List props,
                                         Date maxUpdTime, String dsNm){

        this.idMarcoTemplate = idMarcoTemplate
        this.props = props
        this.dsNm = dsNm
        this.maxUpdTime = maxUpdTime
    }

    @Override
    void execute(SqlRow row) {
        Map obj = [:]
        Map key = [:]
        for (int k = 0; k < props.size(); k++) {
            if (props.get(k).get("prop_name").equals(MACRO_IDX_ID)
                    || props.get(k).get("prop_name").equals(MACRO_IDX_VAL)
                    || props.get(k).get("prop_name").equals(MACRO_END_DT)
                    || props.get(k).get("prop_name").equals(MACRO_UPD_TIME)) {

                setPro(obj, props.get(k), row)
            }
            if (props.get(k).get("prop_name").equals(MACRO_IDX_ID)
                    || props.get(k).get("prop_name").equals(MACRO_END_DT)) {

                setPro(key, props.get(k), row)
            }
        }
        try{
            if (null == idMarcoTemplate.findOne(new Query(Criteria.where(MACRO_IDX_ID).is(key.get(MACRO_IDX_ID))),
                    Map.class, MongoDBConfigConstants.IDX_MARCO)) {
                idMarcoTemplate.save(obj, MongoDBConfigConstants.IDX_MARCO)
            } else {
                def up = EntityUtil.objectToUpdate(obj)
                idMarcoTemplate.updateFirst(new Query(Criteria.where(MACRO_IDX_ID).is(key.get(MACRO_IDX_ID))), up, MongoDBConfigConstants.IDX_MARCO)
            }
            Date updTime = (Date) obj.get(MACRO_UPD_TIME)
            if (updTime.compareTo(maxUpdTime) > 0) {
                maxUpdTime = updTime
                // 保存最新更新时间
                saveMacroMaxUpdTime(maxUpdTime, dsNm)
            }
        }catch (err){
            logger.error("宏观指标数据【"+key.get(MACRO_IDX_ID)+"】保存错误，=========="+obj)
        }

    }

    private void saveMacroMaxUpdTime(Date maxUpdTime, String dsNm) {
		boolean existFlg = false
		if (idMarcoTemplate.collectionExists(MongoDBConfigConstants.IDX_MARCO_UPD)) {
			Map key = ["DS_NAME":dsNm]
			Map old = idMarcoTemplate.findOne(new Query(Criteria.where("DS_NAME").is(dsNm)),
                    Map.class, MongoDBConfigConstants.IDX_MARCO_UPD)
			if (old != null) {
				Date oldUpdTime = (Date)old.get("MAX_UPD_TIME");
				// MAX_UPD_TIME小于最新时间则更新
				if (oldUpdTime.compareTo(maxUpdTime) < 0) {
					Map obj = [:]
					obj.put("DS_NAME",dsNm)
					obj.put("MAX_UPD_TIME", maxUpdTime)
                    def up = EntityUtil.objectToUpdate(obj)
					idMarcoTemplate.updateFirst(new Query(Criteria.where("DS_NAME").is(dsNm)),
                            up, MongoDBConfigConstants.IDX_MARCO_UPD)
				}
				existFlg = true
			}
		}
		if (!existFlg) {
			Map obj = [:]
			obj.put("DS_NAME",dsNm)
			obj.put("MAX_UPD_TIME", maxUpdTime)
			idMarcoTemplate.save(obj, MongoDBConfigConstants.IDX_MARCO_UPD)
		}
	}

    private void setPro(Map obj, Map prop, SqlRow row) {
        if (prop.get("prop_typ") == "Decimal") {
            Double val = row.getDouble(prop.get("prop_name"))
            if (val==null) {
                obj.put(prop.get("prop_name"), null)
            } else {
                obj.put(prop.get("prop_name"), val)
            }
        }
        if (prop.get("prop_typ") == "String") {
            String val = row.getString(prop.get("prop_name"))
            if (val.isEmpty()) {
                obj.put(prop.get("prop_name"), null)
            } else {
                obj.put(prop.get("prop_name"), val)
            }
        }
        if (prop.get("prop_typ") == "DateTime") {
            def dateTime = row.getTimestamp(prop.get("prop_name"))
            if (dateTime == null) {
                obj.put(prop.get("prop_name"), null)
            } else {
                def val = new Date(dateTime.getTime())
                obj.put(prop.get("prop_name"), val)
            }
        }
    }

}
