package com.bigdata.datacenter.datasync.service.impl

import com.avaje.ebean.EbeanServer
import com.avaje.ebean.SqlQuery
import com.avaje.ebean.SqlRow
import com.bigdata.datacenter.metadata.service.RawSQLService
import com.bigdata.datacenter.metadata.service.ResultHandler

import org.apache.log4j.Logger
import org.springframework.data.mongodb.core.MongoOperations
import com.bigdata.datacenter.datasync.utils.constants.EsConfigConstants

import java.util.function.Consumer

/**
 * 专题Es数据

 */
class SubjectEsIncUpdParamsResultHandler implements ResultHandler{
    private static final Logger logger = Logger.getLogger(SubjectEsIncUpdParamsResultHandler.class)

    def MongoOperations subjectTemplate;
    def RawSQLService rawSQLService;
    def String colNm;
    def String sql;  
    def Map<String, Long> paramMap;
    def  List<String> paramNames;
    def List<Map<String, Object>> props;
    def opType;
    def EbeanServer dbEbeanServer;

    SubjectEsIncUpdParamsResultHandler(){}

    SubjectEsIncUpdParamsResultHandler(MongoOperations subjectTemplate, RawSQLService rawSQLService,
                                  String colNm,String sql, HashMap<String, Long> paramMap,
                                  List<String> paramNames,
                                  List<Map<String, Object>> props,String opType){
        this.subjectTemplate = subjectTemplate;
        this.rawSQLService = rawSQLService;
        this.colNm = colNm;
        this.sql = sql;
        this.paramMap = paramMap;
        this.paramNames = paramNames;
        this.props =  props;
        this.opType = opType;
    }
    @Override
    void execute(SqlRow row){
        logger.debug("数据入库开始");
        def start = System.currentTimeMillis()
        Map<String,Object> params = new HashMap<String,Object>()
        paramMap.each{key,value->
            params.put(key,row.get(key))
        }   
        saveToMgDB(colNm,params)   
        def end = System.currentTimeMillis()
        logger.debug("数据入库完成，耗时："+(end-start))
    }
   
    //保存到mongo
    void saveToMgDB(String colNm,Map<String,Object> params){
        if(dbEbeanServer != null){
            ResultHandler resultHandler =new SubjectEsIncrementalResultHandler(subjectTemplate,props,colNm,paramNames,EsConfigConstants.UPD_TYPE)
            SqlQuery query = dbEbeanServer.createSqlQuery(sql);
            if(params) {
                params.each { key, value->
                    query.setParameter(key, value)
                }
            }
            query.findEach(new Consumer<SqlRow>() {
                @Override
                public void accept(SqlRow row) {
                    resultHandler.execute(row)
                }
            })
        }else{
            rawSQLService.queryRawSql(sql,params,
                    new SubjectEsIncrementalResultHandler(subjectTemplate,props,colNm,paramNames,EsConfigConstants.UPD_TYPE))
        }
    }
}
