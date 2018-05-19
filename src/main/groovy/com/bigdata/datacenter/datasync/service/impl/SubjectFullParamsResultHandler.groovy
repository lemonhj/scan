package com.bigdata.datacenter.datasync.service.impl

import com.avaje.ebean.SqlRow
import com.bigdata.datacenter.datasync.utils.DateUtils
import com.bigdata.datacenter.datasync.utils.MongoUtils
import com.bigdata.datacenter.metadata.service.RawSQLService
import com.bigdata.datacenter.metadata.service.ResultHandler
import org.apache.log4j.Logger
import org.springframework.data.mongodb.core.MongoOperations

/**
 * 专题静态数据
 * Created by qq on 2017/5/24.
 */
class SubjectFullParamsResultHandler implements ResultHandler{
    private static final Logger logger = Logger.getLogger(SubjectFullParamsResultHandler.class)

    def MongoOperations subjectTemplate;
    def RawSQLService rawSQLService;
    def String sql;
    def Map<String, Long> paramMap;
    def List<Map<String, Object>> dsIndexLst;
    def List<Map<String, Object>> props;
    def boolean flag;

    SubjectFullParamsResultHandler(){}

    SubjectFullParamsResultHandler(MongoOperations subjectTemplate,RawSQLService rawSQLService,
                                   String sql,HashMap<String, Long> paramMap,
                                   List<Map<String, Object>> dsIndexLst,
                                   List<Map<String, Object>> props,boolean flag){
        this.subjectTemplate = subjectTemplate;
        this.rawSQLService = rawSQLService;
        this.sql = sql;
        this.paramMap = paramMap;
        this.dsIndexLst = dsIndexLst;
        this.props =  props;
        this.flag = flag;
    }
    @Override
    void execute(SqlRow row){
        logger.debug("数据入库开始");
        def start = System.currentTimeMillis()
        String colNm = "";
        Map<String,Object> params = new HashMap<String,Object>()
        paramMap.each{key,value->
              if(value == 30){
                  String paramDate = DateUtils.date2String(row.getDate(key), DateUtils.FORMAT_DATE);
                  colNm += "_"+paramDate
              }else{
                  colNm += "_"+row.get(key)
              }
            params.put(key,row.get(key))
        }
        colNm = colNm.replaceFirst("_", "");
        saveToMgDB(colNm,params)
        def end = System.currentTimeMillis()
        logger.debug("数据入库完成，耗时："+(end-start))
    }

    //保存到mongo
    void saveToMgDB(String colNm,Map<String,Object> params){
        // 更新前删除分区
        if (flag) {
            subjectTemplate.dropCollection(colNm)
        }
        //创建索引
        MongoUtils.addIndex(subjectTemplate,colNm,dsIndexLst);
        rawSQLService.queryRawSql(sql,params,new SubjectResultHandler(subjectTemplate,props,colNm))
    }
}
