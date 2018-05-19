package com.bigdata.datacenter.datasync.service.impl

import com.avaje.ebean.SqlRow
import com.bigdata.datacenter.datasync.utils.DateUtils
import com.bigdata.datacenter.datasync.utils.constants.MongoDBConfigConstants
import com.bigdata.datacenter.datasync.model.mongodb.IndexAll
import com.bigdata.datacenter.datasync.model.mongodb.ParamObj
import com.bigdata.datacenter.metadata.service.ResultHandler
import com.bigdata.datacenter.metadata.utils.EntityUtil
import com.mongodb.DB
import org.springframework.data.mongodb.core.MongoOperations
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.Update

import java.rmi.MarshalledObject
import java.sql.SQLException
import java.sql.Timestamp
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
/**
 * Created by Dan on 2017/6/28.
 */
class AllIndexResultHandler implements ResultHandler{

    private ParamObj[] result
    private List wtExeInsLst
    private String operType
    private Map propsMap = [:]
    private static final String KEY = "Key"
    private static final String Value = "Value"
    private MongoOperations allIndexTemplate

    public AllIndexResultHandler(ParamObj[] result, Map propsMap, List wtExeInsLst, String operType,
                                 MongoOperations allIndexTemplate) {
        this.result= result
        this.wtExeInsLst = wtExeInsLst
        this.operType = operType
        this.propsMap = propsMap
        this.allIndexTemplate = allIndexTemplate
    }

    @Override
    void execute(SqlRow row) {
        String key = ""
        // 组建Key值
        for (int i = 0; i < result.length; i++) {
            String keyType = getProType(result[i].getName(), propsMap)
            // 必要的参数字段值没有在数据源结果集中
            if("".equals(keyType)) {
                //throw new SQLException("Index param is not exist in ds field.");
            }
            if (keyType.equals("Decimal")) {
                key += "_" + new Double(row.getDouble(result[i].getName())).longValue();
            }
            if (keyType.equals("String")) {
                key += "_" + row.getString(result[i].getName());
            }
            if (keyType.equals("DateTime")) {
                Timestamp dateTime = row.getTimestamp(result[i].getName());
                Date val = new Date(dateTime.getTime());
                key += "_" + DateUtils.date2String(val, DateUtils.FORMAT_DATETIME);
            }
        }
        for (int j = 0; j < wtExeInsLst.size(); j++) {
            Map obj = [:]
            Map indexObj = wtExeInsLst.get(j)
            String kyeTmp = indexObj.get("idx_id") + key
            // Key值全部大写
            kyeTmp = kyeTmp.toUpperCase()
            obj.put(KEY, kyeTmp)
            // 获取指标值
            if (getProType(indexObj.get("fld_name"), propsMap).equals("Decimal")) {
                Double val = row.getDouble(indexObj.get("fld_name"))
                if (val == null) {
                    obj.put(Value, null)
                } else {
                    obj.put(Value, val)
                }
            }
            if (getProType(indexObj.get("fld_name"),propsMap).equals("String")) {
                String val = row.getString(indexObj.get("fld_name"))
                if (val == "" || val == null) {
                    obj.put(Value, null)
                } else {
                    obj.put(Value, val)
                }
            }
            if (getProType(indexObj.get("fld_name"),propsMap).equals("DateTime")) {
                Timestamp dateTime = row.getTimestamp(indexObj.get("fld_name"))
                if (dateTime == "" || dateTime == null) {
                    obj.put(Value, null)
                } else {
                    Date val = new Date(dateTime.getTime())
                    obj.put(Value, val)
                }
            }
            // 保存数据到mongoDB
            if (operType.equals("save")) {
                //allIndexTemplate.save(obj, MongoDBConfigConstants.INDEX_ALL_DB)
                def up = EntityUtil.objectToUpdate(obj)
                allIndexTemplate.updateFirst(new Query(Criteria.where(KEY).is(kyeTmp)), up,
                        MongoDBConfigConstants.INDEX_ALL_DB)
            } else if (operType.equals("update")) {
                def up = EntityUtil.objectToUpdate(obj)
                allIndexTemplate.updateFirst(new Query(Criteria.where(KEY).is(kyeTmp)), up,
                        MongoDBConfigConstants.INDEX_ALL_DB)
            }
        }

    }

    private String getProType(String fldName, Map prosMap) {
		String rtn = ""
		Map prop = prosMap.get(fldName)
		if (null != prop) {
			rtn = prop.get("prop_typ")
		}
		return rtn
	}
}
