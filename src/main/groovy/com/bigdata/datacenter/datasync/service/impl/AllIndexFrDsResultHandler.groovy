package com.bigdata.datacenter.datasync.service.impl

import com.avaje.ebean.SqlRow
import com.bigdata.datacenter.datasync.core.data.MongoTemplatePool
import com.bigdata.datacenter.datasync.utils.DateUtils
import com.bigdata.datacenter.datasync.utils.constants.MongoDBConfigConstants
import com.bigdata.datacenter.datasync.model.mongodb.DsStatus
import com.bigdata.datacenter.datasync.model.mongodb.IndexAll
import com.bigdata.datacenter.datasync.model.mongodb.ParamObj
import com.bigdata.datacenter.metadata.service.ResultHandler
import com.bigdata.datacenter.metadata.utils.EntityUtil
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.data.mongodb.core.MongoOperations
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.Update

import java.sql.SQLException
import java.sql.Timestamp
import java.util.concurrent.ThreadPoolExecutor

/**
 * Created by Dan on 2017/7/3.
 */
class AllIndexFrDsResultHandler implements  ResultHandler{

	private Map indexObj
	private ParamObj[] result
	private Map paramMap = [:]
	private List wtExeInsLst
	private String operType
	private ThreadPoolExecutor producerPool
	private Map propsMap = [:]
	private ParamObj[] dsResult
	private MongoOperations allIndexTemplate
    MongoTemplatePool mongoTemplatePool

	public AllIndexFrDsResultHandler(Map indexObj, ParamObj[] result, Map paramMap, List wtExeInsLst,
		   String operType, ThreadPoolExecutor producerPool, Map propsMap,
			ParamObj[] dsResult, MongoOperations allIndexTemplate, MongoTemplatePool mongoTemplatePool) {
		this.indexObj = indexObj
		this.result = result
		this.paramMap = paramMap
		this.wtExeInsLst = wtExeInsLst
		this.operType = operType
		this.producerPool = producerPool
		this.propsMap = propsMap
		this.dsResult = dsResult
		this.allIndexTemplate = allIndexTemplate
		this.mongoTemplatePool = mongoTemplatePool
	}

	public AllIndexFrDsResultHandler(){}

	@Override
	void execute(SqlRow row) {
		String colNm = ""
		if (indexObj.get("ds_typ") == 7) {
			//增量的情况就没有分区
			colNm = indexObj.get("ds_eng_name")
			Map keys = [:]
			for (int j = 0; j < dsResult.length; j++) {
				def paramTypes = paramMap.get(dsResult[j].getName())
				if (paramTypes == 10) {
					Long param = row.getDouble(dsResult[j].getName())
					// 10:数值
					keys.put(dsResult[j].getName(), param)
				} else if (paramTypes == 20) {
					String param = row.getString(dsResult[j].getName())
					// 20:字符
					keys.put(dsResult[j].getName(), param)
				} else if (paramTypes == 30) {
					Timestamp dateTime = row.getTimestamp(dsResult[j].getName())
					Date val = new Date(dateTime.getTime())
					// 30:日期
					keys.put(dsResult[j].getName(), val)
				}
			}
			execIndexObj_frDs(wtExeInsLst, operType, propsMap, indexObj.get("ds_eng_name"), colNm, result,
					String.valueOf(indexObj.get("obj_id").intValue()), keys)
		} else {
			for (int j = 0; j < dsResult.length; j++) {
				def paramTypes = paramMap.get(dsResult[j].getName())
				if (paramTypes == 10) {
					Long param = row.getLong(dsResult[j].getName())
					// 10:数值
					colNm += "_" + String.valueOf(param)
				} else if (paramTypes == 20) {
					String param = row.getString(dsResult[j].getName())
					// 20:字符
					colNm += "_" + param;
				} else if (paramTypes == 30) {
					Timestamp dateTime = row.getTimestamp(dsResult[j].getName())
					Date val = new Date(dateTime.getTime())
					String paramDate = DateUtils.date2String(val,DateUtils.FORMAT_DATE)
					colNm += "_" + paramDate
				}
			}
			colNm = colNm.replaceFirst("_", "")
			execIndexObj_frD(wtExeInsLst, operType, propsMap, indexObj.get("ds_eng_name"), colNm,
					result, String.valueOf(indexObj.get("obj_id").intValue()))
		}
	}

	def execIndexObj_frD(List wtExeInsLst, String operType, Map propsMap,
              String dsEngName, String colName, ParamObj[] result, String objId) {
        List cur = []
        MongoOperations dsColTemplate = mongoTemplatePool.getByName(dsEngName)
        cur = dsColTemplate.findAll(DsStatus.class, colName)
        for ( var in cur) {
            String key = ""
            for (int i = 0; i < result.length; i++) {
                String keyType = getProType(result[i].getName(),propsMap)
                // 必要的参数字段值没有在数据源结果集中
                if("".equals(keyType)) {
                    throw new SQLException("Index param is not exist in ds field.")
                }
                if (keyType.equals("Decimal")) {
                    key += "_" + ((Double) var.get(result[i].getName())).longValue()
                }
                if (keyType.equals("String")) {
                    key += "_" + var.get(result[i].getName())
                }
                if (keyType.equals("DateTime")) {
                    Date dateTime = (Date) var.get(result[i].getName());
                    key += "_" + DateUtils.date2String(dateTime, DateUtils.FORMAT_DATETIME)
                }
            }

            for (int j = 0; j < wtExeInsLst.size(); j++) {
                Map obj = [:]
                Map indexObj = wtExeInsLst.get(j)
                String colNm = MongoDBConfigConstants.INDEX_ALL_DB
                String kyeTmp = indexObj.get("IDX_ID").intValue() + key
                // Key值全部大写
                kyeTmp = kyeTmp.toUpperCase()
                obj.put(AllIndexServiceImpl.KEY, kyeTmp)
                obj.put(AllIndexServiceImpl.Value, var.get(indexObj.get("FLD_NAME")))
                // 保存数据到mongoDB
                if ((AllIndexServiceImpl.OPER_TYPE_SAVE).equals(operType)) {
                    allIndexTemplate.save(obj, colNm)
                } else if ((AllIndexServiceImpl.OPER_TYPE_UPDATE).equals(operType)) {
                    def up = EntityUtil.objectToUpdate(obj)
                    allIndexTemplate.updateFirst(new Query(Criteria.where(AllIndexServiceImpl.KEY).is(kyeTmp)), up, colNm)
                }
            }

        }
		for (int j = 0; j < wtExeInsLst.size(); j++) {
			Map indexObj = wtExeInsLst.get(j)
			addIdxNmToStatus(indexObj.get("DS_ENG_NAME"),indexObj.get("IDX_NAME"),indexObj.get("FLD_NAME"));
		}

    }
	def addIdxNmToStatus(String dsName,String idxName,String fldNm) {
		MongoOperations dsStatusTemplate = mongoTemplatePool.getByName(MongoDBConfigConstants.IDX_STATUS_DB)
        Map dsStatus = dsStatusTemplate.findOne(new Query(Criteria.where("DS_NAME").is(dsName)), Map.class)
		if (null != dsStatus) {
			List idxNms = dsStatus.get("IDX_NAMES")
			Map obj = [:]
			obj.put("FLD_NAME", fldNm)
			obj.put("IDX_NAME", idxName)
			if (idxNms == null) {
				idxNms = []
			} else {
				if (idxNms.contains(obj)) {
					return
				}
			}
			idxNms.add(obj)
			dsStatus.put("IDX_NAMES", idxNms);
		}
		def up = EntityUtil.objectToUpdate(dsStatus)
        dsStatusTemplate.updateFirst(new Query(Criteria.where("DS_NAME").is(dsName)), up, dsName)
	}

	private void execIndexObj_frDs(List wtExeInsLst, String operType, Map propsMap, String dsEngName,
		String colName, ParamObj[] result, String objId,Map keys) {
		List cur = []
		MongoOperations dsColTemplate = mongoTemplatePool.getByName(dsEngName)
		//查询所有的数据
		cur = dsColTemplate.find(new Query(Criteria.where(keys.keySet()[0]).is(keys.get(keys.keySet()[0]))), Map.class, colName)
		for (var in cur) {
			// 组建Key值
			String key = ""
			for (int i = 0; i < result.length; i++) {
				String keyType = getProType(result[i].getName(), propsMap);
				// 必要的参数字段值没有在数据源结果集中
				if ("".equals(keyType)) {
					throw new SQLException("Index param is not exist in ds field.");
				}
				if (keyType.equals("Decimal")) {
					key += "_" + ((Double) var.get(result[i].getName())).longValue();
				}
				if (keyType.equals("String")) {
					key += "_" + var.get(result[i].getName());
				}
				if (keyType.equals("DateTime")) {
					Date dateTime = (Date) var.get(result[i].getName());
					key += "_" + DateUtils.date2String(dateTime, DateUtils.FORMAT_DATETIME);
				}
			}
			for (int j = 0; j < wtExeInsLst.size(); j++) {
				Map obj = [:]
				Map indexObj = wtExeInsLst.get(j)
				// 分片保存指标
				//String colNm = MongoDBConfigConstants.INDEX_ALL_DB
				//indexDsCol = indexDsDb.getCollection(colNm)
				String kyeTmp = indexObj.get("idx_id").intValue() + key
				// Key值全部大写
				kyeTmp = kyeTmp.toUpperCase()
				obj.put("Key", kyeTmp)
				obj.put("Value", var.get(indexObj.get("FLD_NAME")))
				// 保存数据到mongoDB
				if (AllIndexServiceImpl.OPER_TYPE_SAVE.equals(operType)) {
					def up = EntityUtil.objectToUpdate(obj)
					allIndexTemplate.updateFirst(new Query(Criteria.where("Key").is(kyeTmp)), up,
							MongoDBConfigConstants.INDEX_ALL_DB)
				} else if (AllIndexServiceImpl.OPER_TYPE_UPDATE.equals(operType)) {
					def up = EntityUtil.objectToUpdate(obj)
					allIndexTemplate.updateFirst(new Query(Criteria.where("Key").is(kyeTmp)), up,
							MongoDBConfigConstants.INDEX_ALL_DB)
				}
			}
		}
		// 保存字段属性
		for (int j = 0; j < wtExeInsLst.size(); j++) {
			IndexAll indexObj = wtExeInsLst.get(j);
			addIdxNmToStatus(indexObj.getDS_ENG_NAME(), indexObj.getIDX_NAME(), indexObj.getFLD_NAME());
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
