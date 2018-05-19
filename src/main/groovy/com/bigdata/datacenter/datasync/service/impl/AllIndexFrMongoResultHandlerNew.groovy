package com.bigdata.datacenter.datasync.service.impl

import com.avaje.ebean.SqlRow
import com.bigdata.datacenter.datasync.core.data.MongoTemplatePool
import com.bigdata.datacenter.datasync.utils.DateUtils
import com.bigdata.datacenter.datasync.utils.constants.MongoDBConfigConstants
import com.bigdata.datacenter.datasync.model.mongodb.ParamObj
import com.bigdata.datacenter.metadata.service.ResultHandler
import com.bigdata.datacenter.metadata.utils.EntityUtil
import org.apache.log4j.Logger
import org.springframework.data.mongodb.core.MongoOperations
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query

import java.sql.SQLException
import java.sql.Timestamp

/**
 * Created by Dan on 2017/7/3.
 */
class AllIndexFrMongoResultHandlerNew implements  ResultHandler{
	private static final Logger logger = Logger.getLogger(AllIndexFrMongoResultHandlerNew.class)
	private Map indexObj
	private ParamObj[] result
	private Map paramMap = [:]
	private List wtExeInsLst
	private Map propsMap = [:]
	private MongoOperations allIndexTemplate
    MongoTemplatePool mongoTemplatePool

	public AllIndexFrMongoResultHandlerNew(Map<String,Object> indexObj, ParamObj[] result,
										   Map<String,Object> paramMap,
										   List<Map<String,Object>> wtExeInsLst,
										   Map<String,Map<String,Object>> propsMap,
										   MongoOperations allIndexTemplate,
										   MongoTemplatePool mongoTemplatePool) {
		this.indexObj = indexObj
		this.result = result
		this.paramMap = paramMap
		this.wtExeInsLst = wtExeInsLst
		this.propsMap = propsMap
		this.allIndexTemplate = allIndexTemplate
		this.mongoTemplatePool = mongoTemplatePool
	}

	public AllIndexFrMongoResultHandlerNew(){}

	@Override
	void execute(SqlRow row) {
		if (indexObj.get("ds_typ") == 7) {
			//增量的情况就没有分区
			Map<String,Object> keys = [:]
			paramMap.each { key, value ->
				if (value == 30) {
					Timestamp dateTime = row.getTimestamp(key)
					Date val = new Date(dateTime.getTime())
					// 30:日期
					keys.put(key, val)
				} else {
					keys.put(key, row.get(key))
				}
			}
			saveIdxFormMongo(wtExeInsLst, propsMap, indexObj.DS_ENG_NAME, result, keys)
		} else {
			String idxVal = ""
			paramMap.each { key, value ->
				if (value == 30) {
					String paramDate = DateUtils.date2String((Date)row.get(key), DateUtils.FORMAT_DATE);
					idxVal += "_" + paramDate
				} else {
					idxVal += "_" + row.get(key)
				}
			}
			idxVal = idxVal.replaceFirst("_", "");
			saveIdxFormMongo(wtExeInsLst, propsMap, indexObj.DS_ENG_NAME,result,["ext_section_val":idxVal])
		}
	}

	/**
	 * 从mongo保存指标数据
	 * @param wtExeInsLst
	 * @param operType
	 * @param propsMap
	 * @param dsEngName
	 * @param colName
	 * @param result
	 * @return
	 */
	 void saveIdxFormMongo(List<Map<String,Object>> wtExeInsLst,Map<String,Map<String,Object>> propsMap,
              				String colName, ParamObj[] result) {
        MongoOperations dsColTemplate = mongoTemplatePool.getByName(MongoDBConfigConstants.SUB_DATA_SOURCE)
		List<Map<String,Object>> cur = dsColTemplate.findAll(Map.class, colName)
        for ( var in cur) {
            String key = ""
            for (int i = 0; i < result.length; i++) {
                String keyType = getProType(result[i].getName(),propsMap)
				// 必要的参数字段值没有在数据源结果集中
				if("" == keyType) {
					throw new SQLException("Index param is not exist in ds field.")
				}
				if (keyType == "DateTime") {
					Date dateTime = (Date) var.get(result[i].getName());
					key += "_" + DateUtils.date2String(dateTime, DateUtils.FORMAT_DATE)
				}else{
					key += "_" + var.get(result[i].getName())
				}
            }

			wtExeInsLst.each {
				String keyTmp = it.IDX_ID + key
				// Key值全部大写
				keyTmp = keyTmp.toUpperCase()
				Map<String,Object> obj = [:]
				obj.put(AllIndexServiceImplNew.KEY, keyTmp)
				obj.put(AllIndexServiceImplNew.Value, var.get(it.FLD_NAME))

				try{
					// 保存数据到mongoDB
					def up = EntityUtil.objectToUpdate(obj)
					allIndexTemplate.upsert(new Query(Criteria.where(AllIndexServiceImplNew.KEY).is(keyTmp)), up,
							MongoDBConfigConstants.INDEX_ALL_DB)
				}catch (err){
					logger.error("指标数据【"+keyTmp+"】保存错误，======="+obj)
				}
			}
        }
		addIdxNmToStatus(wtExeInsLst);
    }

	 void saveIdxFormMongo(List<Map<String,Object>> wtExeInsLst,Map<String,Map<String,Object>> propsMap,String colName,
						   ParamObj[] result,Map<String,Object> keys) {
		Criteria criteria = new Criteria();
		if(keys!=null){
			keys.each {key,value->
				criteria.andOperator(Criteria.where(key).is(value))
			}
		}
		MongoOperations dsColTemplate = mongoTemplatePool.getByName(MongoDBConfigConstants.SUB_DATA_SOURCE)
		//查询所有的数据
		List<Map<String,Object>> cur = dsColTemplate.find(new Query(criteria), Map.class, colName)
		for (var in cur) {
			// 组建Key值
			String key = ""
			for (int i = 0; i < result.length; i++) {
				String keyType = getProType(result[i].getName(), propsMap);
				// 必要的参数字段值没有在数据源结果集中
				if ("" == keyType) {
					throw new SQLException("Index param is not exist in ds field.");
				}
				if (keyType == "DateTime") {
					Date dateTime = (Date) var.get(result[i].getName());
					key += "_" + DateUtils.date2String(dateTime, DateUtils.FORMAT_DATE);
				}else{
					key += "_" + var.get(result[i].getName());
				}
			}

			wtExeInsLst.each {
				String keyTmp = it.IDX_ID + key
				// Key值全部大写
				keyTmp = keyTmp.toUpperCase()
				Map<String,Object> obj = [:]
				obj.put(AllIndexServiceImplNew.KEY, keyTmp)
				obj.put(AllIndexServiceImplNew.Value, var.get(it.FLD_NAME))

				try{
					// 保存数据到mongoDB
					def up = EntityUtil.objectToUpdate(obj)
					allIndexTemplate.upsert(new Query(Criteria.where(AllIndexServiceImplNew.KEY).is(keyTmp)), up,
							MongoDBConfigConstants.INDEX_ALL_DB)
				}catch (err){
					logger.error("指标数据【"+keyTmp+"】保存错误，==========="+obj)
				}

			}
		}
		// 保存字段属性
		addIdxNmToStatus(wtExeInsLst)
	}

	/**
	 * 更新指标状态表中数据源中指标集合
	 * @param wtExeInsLst
	 * @return
	 */
	def addIdxNmToStatus(List<Map<String,Object>> wtExeInsLst) {
		if(wtExeInsLst==null || wtExeInsLst.size()<1){
			return
		}
		Map indexObj = wtExeInsLst.get(0)
		def dsName = indexObj.DS_ENG_NAME
		MongoOperations dsStatusTemplate = mongoTemplatePool.getByName(MongoDBConfigConstants.IDX_STATUS_DB)
		Map<String,Object> dsStatus = dsStatusTemplate.findOne(new Query(Criteria.where("DS_NAME").is(dsName)), Map.class,MongoDBConfigConstants.IDX_STATUS_DB)
		if(null!=dsStatus){
			List idxNms = dsStatus.get("IDX_NAMES")
			for(def it : wtExeInsLst) {
				Map obj = [:]
				obj.put("FLD_NAME", it.FLD_NAME)
				obj.put("IDX_NAME", it.IDX_NAME)
				if(idxNms == null){
					idxNms = []
				}else{
					if(idxNms.contains(obj)){
						continue
					}
				}
				idxNms.add(obj)
			}
			dsStatus.put("IDX_NAMES", idxNms);
			def up = EntityUtil.objectToUpdate(dsStatus)
			dsStatusTemplate.updateFirst(new Query(Criteria.where("DS_NAME").is(dsName)), up, MongoDBConfigConstants.IDX_STATUS_DB)
		}
	}

	/**
	 * 根据属性名称获取属性类型
	 * @param fldName
	 * @param prosMap
	 * @return
	 */
	private String getProType(String fldName, Map<String,Map<String,Object>> prosMap) {
		String rtn = ""
		Map<String,Object> prop = prosMap.get(fldName)
		if (null != prop) {
			rtn = prop.get("prop_typ")
		}
		return rtn
	}

}
