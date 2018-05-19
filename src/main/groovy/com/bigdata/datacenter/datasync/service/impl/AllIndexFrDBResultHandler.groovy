package com.bigdata.datacenter.datasync.service.impl

import com.avaje.ebean.SqlRow
import com.bigdata.datacenter.datasync.utils.DateUtils
import com.bigdata.datacenter.datasync.utils.constants.MongoDBConfigConstants
import com.bigdata.datacenter.datasync.model.mongodb.IndexAll
import com.bigdata.datacenter.datasync.model.mongodb.ParamObj
import com.bigdata.datacenter.metadata.service.ResultHandler
import com.bigdata.datacenter.metadata.utils.EntityUtil
import com.mongodb.DB
import org.apache.log4j.Logger
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
class AllIndexFrDBResultHandler implements ResultHandler{

    private static final Logger logger = Logger.getLogger(AllIndexFrDBResultHandler.class)
    private ParamObj[] result
    private List wtExeInsLst
    private Map propsMap = [:]
    private MongoOperations allIndexTemplate
    def MailService mailService

    public AllIndexFrDBResultHandler(ParamObj[] result, Map propsMap, List wtExeInsLst,
                                     MongoOperations allIndexTemplate) {
        this.result= result
        this.wtExeInsLst = wtExeInsLst
        this.propsMap = propsMap
        this.allIndexTemplate = allIndexTemplate
    }

    public AllIndexFrDBResultHandler(ParamObj[] result, Map propsMap, List wtExeInsLst,
                                     MongoOperations allIndexTemplate,MailService mailService) {
        this.result= result
        this.wtExeInsLst = wtExeInsLst
        this.propsMap = propsMap
        this.allIndexTemplate = allIndexTemplate
        this.mailService = mailService
    }

    @Override
    void execute(SqlRow row) {
        if(row == null || row == ""){
            return ;
        }
        String key = ""
        // 组建Key值
        for (int i = 0; i < result.length; i++) {
            String keyType = getProType(result[i].getName(), propsMap)
            // 必要的参数字段值没有在数据源结果集中
            if(keyType == "") {
                //throw new SQLException("Index param is not exist in ds field.");
            }
            if (keyType == "DateTime") {
                Timestamp dateTime = row.getTimestamp(result[i].getName());
                Date val = new Date(dateTime.getTime());
                key += "_" + DateUtils.date2String(val, DateUtils.FORMAT_DATE);
            }else{
                key += "_" + row.get(result[i].getName());
            }
        }
        for (int j = 0; j < wtExeInsLst.size(); j++) {
            Map obj = [:]
            Map indexObj = wtExeInsLst.get(j)
            String fldType = getProType(indexObj.get("fld_name"), propsMap);
            String kyeTmp = indexObj.get("idx_id") + key
            // Key值全部大写
            kyeTmp = kyeTmp.toUpperCase()
            obj.put(AllIndexServiceImplNew.KEY, kyeTmp)
            // 获取指标值
            if (fldType == "Decimal") {
                Double val = row.getDouble(indexObj.get("fld_name"))
                if(val == null){
                    obj.put(AllIndexServiceImplNew.Value, null)
                }else{
                    obj.put(AllIndexServiceImplNew.Value, val)
                }
            }
            if (fldType == "String") {
                String val = row.getString(indexObj.get("fld_name"))
                if(val==null || val == ""){
                    obj.put(AllIndexServiceImplNew.Value, null)
                }else{
                    obj.put(AllIndexServiceImplNew.Value, val)
                }
            }
            if (fldType == "DateTime") {
                Timestamp dateTime = row.getTimestamp(indexObj.get("fld_name"))
                if(dateTime==null){
                    obj.put(AllIndexServiceImplNew.Value, null)
                }else {
                    Date val = new Date(dateTime.getTime())
                    obj.put(AllIndexServiceImplNew.Value, val)
                }
            }
            try{
                // 保存数据到mongoDB
                def up = EntityUtil.objectToUpdate(obj)
                allIndexTemplate.upsert(new Query(Criteria.where(AllIndexServiceImplNew.KEY).is(kyeTmp)), up,
                        MongoDBConfigConstants.INDEX_ALL_DB)
            }catch (err){
                logger.error("指标数据【"+kyeTmp+"】保存错误，======"+obj)
                mailService.sendMail("指标数据【"+kyeTmp + "】数据保存失败","专题数据源【"+kyeTmp+ "】数据保存失败，错误信息为："+err+",错误数据为："+obj)
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
