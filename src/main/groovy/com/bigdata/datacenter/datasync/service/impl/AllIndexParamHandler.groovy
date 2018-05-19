package com.bigdata.datacenter.datasync.service.impl

import com.avaje.ebean.SqlRow
import com.bigdata.datacenter.datasync.core.data.MongoTemplatePool
import com.bigdata.datacenter.datasync.utils.DateUtils
import com.bigdata.datacenter.datasync.utils.PropertiesUtil
import com.bigdata.datacenter.datasync.utils.constants.MongoDBConfigConstants
import com.bigdata.datacenter.datasync.model.mongodb.ParamObj
import com.bigdata.datacenter.metadata.service.RawSQLService
import com.bigdata.datacenter.metadata.service.ResultHandler
import com.bigdata.datacenter.metadata.utils.EntityUtil
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.log4j.Logger
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.MongoOperations
import org.springframework.data.mongodb.core.query.Query

import java.util.concurrent.ThreadPoolExecutor
import java.util.regex.Matcher
import java.util.regex.Pattern

/**
 * Created by Dan on 2017/6/27.
 */
class AllIndexParamHandler implements ResultHandler{
    private static final Logger logger = Logger.getLogger(AllIndexParamHandler.class)

    private String dsSql
    private Map indexObj
    private ParamObj[] result
    private Map paramMap
    private List props
    private List wtExeInsLst
    private String operType
    private ThreadPoolExecutor producerPool
    private db_type = PropertiesUtil.getProperty("db.type")
    private MongoOperations allIndexTemplate
    private RawSQLService rawSQLService
    private MongoTemplatePool mongoTemplatePool

    public AllIndexParamHandler(Map indexObj, ParamObj[] result, Map paramMap, List props,
                                List wtExeInsLst, MongoOperations allIndexTemplate, String operType,
                                ThreadPoolExecutor producerPool, String dsSql, RawSQLService rawSQLService,
                                MongoTemplatePool mongoTemplatePool) {
        this.indexObj = indexObj
        this.result = result
        this.paramMap = paramMap
        this.props = props
        this.wtExeInsLst = wtExeInsLst
        this.operType = operType
        this.producerPool = producerPool
        this.dsSql = dsSql
        this.allIndexTemplate = allIndexTemplate
        this.rawSQLService = rawSQLService
        this.mongoTemplatePool = mongoTemplatePool
    }

    @Override
    void execute(SqlRow row) {
        for (int j = 0; j < result.length; j++) {
            def paramTypes = paramMap.get(result[j].getName())
            // 替换sql语句中的参数表达式
            Pattern pattern = Pattern.compile('[\\$]*\\{[\\s]*' + result[j].getName() + '[\\s]*\\}')
            Matcher matcher = pattern.matcher(dsSql);
            if (paramTypes == 10) {
                Long param = row.getLong(result[j].getName())
                // 10:数值
                dsSql = matcher.replaceAll(String.valueOf(param))
            } else if (paramTypes == 20) {
                String param = row.getString(result[j].getName())
                // 20:字符
                dsSql = matcher.replaceAll("'" +  param + "'")
            } else if (paramTypes == 30) {
                String paramDate = DateUtils.date2String(row.getDate(result[j].getName()), DateUtils.FORMAT_DATE)
                // 30:日期
                if (db_type.equals("oracle")) {
                    dsSql = matcher.replaceAll("TO_DATE('" + paramDate + "', 'YYYY-MM-DD')")
                }
                if (db_type.equals("sqlserver")) {
                    dsSql = matcher.replaceAll("Convert(datetime ,'" + paramDate + "')")
                }
                if (db_type.equals("mysql")) {
                    dsSql = matcher.replaceAll("str_to_date('" + paramDate + "', '%Y-%m-%d')")
                }
            }
        }
        producerPool.execute(new ThreadPoolTask(wtExeInsLst, props, dsSql, operType))
    }

    /*多线程保存数据至mongoDB*/
	private class ThreadPoolTask implements Runnable, Serializable{

		private List wtExeInsLst
		private List props
		private String sql
		private String operType

		public ThreadPoolTask(List wtExeInsLst, List props, String sql, String operType) {
			this.wtExeInsLst = wtExeInsLst
			this.props = props
			this.sql = sql
			this.operType = operType
		}

		@Override
		public void run() {
			try {
				execIndexObj(wtExeInsLst, props, sql, operType)
				Thread.sleep(1)
			} catch (Exception e) {
				logger.error(e.toString())
			}
		}
	}

    private void execIndexObj(List wtExeInsLst, List props,String sql, String operType) {
		// 带有参数的情况下
		ObjectMapper mapper = new ObjectMapper()
		ParamObj[] result =mapper.readValue(wtExeInsLst.get(0).get("idx_param"), ParamObj[].class)
		// 按照参数名称字母排序组成mongoDB集合名称
	    Collections.sort(Arrays.asList(result))
	    Map propsMap = getProsMap(props)
	    AllIndexResultHandler dsRow = new AllIndexResultHandler(result, propsMap, wtExeInsLst, operType, allIndexTemplate)
		rawSQLService.queryRawSql(sql, dsRow)

		for (int j = 0; j < wtExeInsLst.size(); j++) {
			Map indexObj = wtExeInsLst.get(j)
			addIdxNmToStatus(indexObj.get("ds_eng_name"),indexObj.get("idx_name"),indexObj.get("fld_name"))
		}
	}

    private void addIdxNmToStatus(String dsName,String idxName,String fldNm) {
		MongoOperations idxStatusTemplate = mongoTemplatePool.getByName(MongoDBConfigConstants.IDX_STATUS_DB)
		Map dsStatus = idxStatusTemplate.findOne(new Query(Criteria.where("DS_NAME").is(dsName)), Map.class)
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
			dsStatus.put("IDX_NAMES", idxNms)
		}
        def up = EntityUtil.objectToUpdate(dsStatus)
		idxStatusTemplate.updateFirst(new Query(Criteria.where("DS_NAME").is(dsName)), up,
                MongoDBConfigConstants.IDX_STATUS_DB)
	}

    private Map getProsMap(List props) {
		Map map = [:]
		for (int k = 0; k < props.size(); k++) {
			map.put(props.get(k).get("prop_name"), props.get(k))
		}
		return map
	}
}
