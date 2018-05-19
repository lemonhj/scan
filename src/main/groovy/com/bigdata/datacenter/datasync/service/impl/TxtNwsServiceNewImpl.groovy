package com.bigdata.datacenter.datasync.service.impl

import com.bigdata.datacenter.datasync.core.data.MongoTemplatePool
import com.bigdata.datacenter.datasync.service.ScanService
import com.bigdata.datacenter.datasync.service.TxtAndRrpStatusService
import com.bigdata.datacenter.datasync.service.XmlRawSQLService
import com.bigdata.datacenter.datasync.utils.DateUtils
import com.bigdata.datacenter.datasync.utils.ESearchHelper;
import com.bigdata.datacenter.datasync.utils.MongoUtils
import com.bigdata.datacenter.datasync.utils.PropertiesUtil
import com.bigdata.datacenter.datasync.utils.constants.MongoDBConfigConstants
import com.bigdata.datacenter.datasync.model.mongodb.Nws
import com.bigdata.datacenter.datasync.model.mongodb.NwsPros
import com.bigdata.datacenter.metadata.service.RawSQLService
import com.bigdata.datacenter.metadata.utils.StringUtils

import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.data.domain.Sort
import org.springframework.data.mongodb.core.MongoOperations
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.stereotype.Service

/**
 * 新闻服务
 * Created by qq on 2017/5/19.
 */
@Service(value = "txt-nws")
class TxtNwsServiceNewImpl implements ScanService{
    private static final Logger logger = Logger.getLogger(TxtNwsServiceNewImpl.class)
    @Autowired
    RawSQLService rawSQLService
    @Autowired
    XmlRawSQLService xmlRawSQLService
    @Autowired
    TxtAndRrpStatusService txtAndRrpStatusService
    @Autowired
    MongoTemplatePool mongoTemplatePool
    MongoOperations txtNwsTemplate;
    //临时保存新闻服务数据源信息
    Map<String,String> dsInfo;
    public static String nwstyp;

    //邮件服务
    @Autowired
    MailService mailService
    /**
     * 增量更新
     */
    @Override
    void incrementalSync() {
        logger.info("新闻服务增量更新开始")
        long startTime = System.currentTimeMillis();
        try{
            initDsInfo(nwstyp)
            txtNwsTemplate = mongoTemplatePool.getByName(dsInfo.get("dsNm"))
            //新闻服务状态修改为正在更新
            txtAndRrpStatusService.updateDsStatus(dsInfo.get("dsNm"),TxtAndRrpStatusService.DS_STAS_UPDATE)
            //获取最新数据时间
            Query query=new Query();
            query.with(new Sort(new Sort.Order(Sort.Direction.DESC,"UPD_TIME")));
            Nws nws = txtNwsTemplate.findOne(query,Nws.class,dsInfo.get("dsNm"))
            if(nws!=null){
                Date maxUpDate = nws.UPD_TIME
                if (PropertiesUtil.getProperty("nws.del").equals("true")) {
                    // 删除数据库中已删除的数据
                    removeDate(maxUpDate);
                }

                String selectNwsByMaxUpDate = PropertiesUtil.getProperty(dsInfo.get("selectNwsByMaxUpDate"));
                List<Map<String, Object>> sqlList = xmlRawSQLService.queryRawSqlByKey("selectDataSrcByNm",[dsNm:selectNwsByMaxUpDate])
                if(sqlList!=null&&sqlList.size()>0){
                    //将clob字段转换为string
                    String sql = StringUtils.ClobToString(sqlList.get(0).SQL_CLAUSE)
                    sql = replaceSql(sql,['${MAXUPDATE}':':MAXUPDATE'])
                    //获取sql的参数
                    List<Map<String, Object>> props = xmlRawSQLService.queryRawSqlByKey("selectDopByObjId",[obj_id:sqlList.get(0).OBJ_ID]);
                    //查询并保存到mongodb
                    rawSQLService.queryRawSql(sql,[MAXUPDATE:maxUpDate],new TxtNwsResultHandler(txtNwsTemplate,dsInfo.get("dsNm"),props,dsInfo.get("nwstyp"),mailService))
                }
            }
            //新闻服务状态修改为正常，表示更新完成
            txtAndRrpStatusService.updateDsStatus(dsInfo.get("dsNm"),TxtAndRrpStatusService.DS_STAS_NORMAL)
        }catch (err){
            logger.error("新闻服务增量更新错误："+ err );
            txtAndRrpStatusService.updateDsStatus(MongoDBConfigConstants.TXT_TIP_DB, TxtAndRrpStatusService.DS_STAS_ERROR)
            mailService.sendMail("新闻服务增量更新失败","【新闻服务】增量更新失败,错误信息："+err);
        }finally{
		    ESearchHelper.closeClient();
        }
        long endTime = System.currentTimeMillis();
        logger.info("新闻服务增量更新完成,共耗时："+(endTime-startTime)+"ms");
    }

    /**
     * 全量更新
     */
    @Override
    void totalSync() {
          logger.info("新闻服务初始化开始")
          long startTime = System.currentTimeMillis();
          try{
              initDsInfo(nwstyp)
              txtNwsTemplate = mongoTemplatePool.getByName(dsInfo.get("dsNm"))
              //新闻服务状态修改为正在更新
               txtAndRrpStatusService.updateDsStatus(dsInfo.get("dsNm"),TxtAndRrpStatusService.DS_STAS_UPDATE)
              //判断新闻服务临时表是否存在，存在则删除
              if(txtNwsTemplate.collectionExists(dsInfo.get("dsNm")+"_tmp")){
                  txtNwsTemplate.dropCollection(dsInfo.get("dsNm")+"_tmp")
              }

              //创建索引
              List<Map<String,Object>> idxList = new ArrayList<Map<String,Object>>();
              idxList.add([INDEX_NAME:'nws_pub_end_idx',PROP_NAME:'PUB_DT',ORDER_RULE:'desc'])
              idxList.add([INDEX_NAME:'nws_pub_end_idx',PROP_NAME:'ENT_TIME',ORDER_RULE:'desc'])
              idxList.add([INDEX_NAME:'ID',PROP_NAME:'ID',ORDER_RULE:'desc'])
              idxList.add([INDEX_NAME:'TYP_CODE',PROP_NAME:'TYP_CODE',ORDER_RULE:"desc"])
              idxList.add([INDEX_NAME:'STOCKS.SECU_ID',PROP_NAME:'STOCKS.SECU_ID',ORDER_RULE:"desc"])
              idxList.add([INDEX_NAME:'MARKS.ID',PROP_NAME:'MARKS.ID',ORDER_RULE:"desc"])
              idxList.add([INDEX_NAME:'MED_NAME',PROP_NAME:'MED_NAME',ORDER_RULE:"desc"])
              idxList.add([INDEX_NAME:'UPD_TIME',PROP_NAME:'UPD_TIME',ORDER_RULE:'desc'])
              idxList.add([INDEX_NAME:'SECT_ID',PROP_NAME:'SECT_ID',ORDER_RULE:"desc"])
              idxList.add([INDEX_NAME:'SECU_ID',PROP_NAME:'SECU_ID',ORDER_RULE:"desc"])
              idxList.add([INDEX_NAME:'INDU_ID',PROP_NAME:'INDU_ID',ORDER_RULE:"desc"])
              idxList.add([INDEX_NAME:'RS_ID',PROP_NAME:'RS_ID',ORDER_RULE:'desc'])
              MongoUtils.addIndex(txtNwsTemplate,dsInfo.get("dsNm")+"_tmp",idxList)

              String selectAllNwsByYearParam = PropertiesUtil.getProperty(dsInfo.get("selectAllNwsByYear"));
              List<Map<String, Object>> list = xmlRawSQLService.queryRawSqlByKey("selectDataSrcByNm",[dsNm:selectAllNwsByYearParam])
              if(list!=null&&list.size()>0){
                  //将clob字段转换为string
                  String sql = StringUtils.ClobToString(list.get(0).SQL_CLAUSE)
                  //替换sql语句中的参数格式
                  sql = replaceSql(sql,['${BEGDATE}':':BEGDATE','${ENDDATE}':':ENDDATE'])
                  //获取sql的参数
                  List<Map<String, Object>> props = xmlRawSQLService.queryRawSqlByKey("selectDopByObjId",[obj_id:list.get(0).OBJ_ID]);
                  // 设置起始日期
                  Map<String,Date> dateTimeParam = new HashMap<String, Date>();
                  Date begDt = DateUtils.string2Date("1990-01-01", DateUtils.FORMAT_DATE);
                  Date endDt = DateUtils.addMonths(begDt, 1);
                  dateTimeParam.put("BEGDATE", begDt);
                  dateTimeParam.put("ENDDATE", endDt);
                  //逐月扫描
                  while (begDt.compareTo(new Date()) <= 0) {
                      //查询数据并保存到mongodb
                      try{
                          rawSQLService.queryRawSql(sql,dateTimeParam,new TxtNwsResultHandler(txtNwsTemplate,dsInfo.get("dsNm")+"_tmp",props,dsInfo.get("nwstyp"),mailService))
                      }catch (e){
                          logger.error("时间段["+dateTimeParam+"]数据查询错误："+ e.getMessage() );
                      }

                      begDt = DateUtils.addMonths(begDt, 1);
                      endDt = DateUtils.addMonths(endDt, 1);
                      dateTimeParam.put("BEGDATE", begDt);
                      dateTimeParam.put("ENDDATE", endDt);
                  }
                  //保存属性类型
                  savePros(props,dsInfo.get("dsNm"));
              }
               //将临时表更名为正常表
              txtNwsTemplate.getCollection(dsInfo.get("dsNm")+"_tmp").rename(dsInfo.get("dsNm"),true)

               //新闻服务状态修改为正常，表示更新完成
               txtAndRrpStatusService.updateDsStatus(dsInfo.get("dsNm"),TxtAndRrpStatusService.DS_STAS_NORMAL)
          }catch (err){
                logger.error("新闻服务初始化错误："+ err.getMessage() );
                txtAndRrpStatusService.updateDsStatus(dsInfo.get("dsNm"), TxtAndRrpStatusService.DS_STAS_ERROR)
                mailService.sendMail("新闻服务初始化失败","【新闻服务】初始化失败,错误信息："+err);
          } finally{
              try{
                  ESearchHelper.closeClient();
              }catch (err){}

          }
		  
		  
           long endTime = System.currentTimeMillis();
           logger.info("新闻服务初始化完成,共耗时："+(endTime-startTime)+"ms");
           mailService.sendMail("新闻服务完成初始化扫描","【新闻服务】初始化完成,共耗时："+(endTime-startTime)+"ms");
    }

    /**
     * 根据新闻获取获取相关的参数
     * @param nwstyp
     * @return
     */
     Map<String,String> initDsInfo(String nwstyp) {
        dsInfo = new HashMap<String, String>();
        dsInfo.put("nwstyp",nwstyp)
        if (nwstyp == null || (!nwstyp.equals("NWS") && !nwstyp.equals("WCJ") && !nwstyp.equals("YCNC"))) {
            logger.error("Param [nwstyp] is null. Please input [NWS] or [WCJ] or [YCNC].");
            return dsInfo;
        }
        dsInfo.put("selectAllNwsByYear", nwstyp.toLowerCase() + ".selectAllNwsByYear");
        dsInfo.put("selectIdByMaxUpDate", nwstyp.toLowerCase() + ".selectIdByMaxUpDate");
        dsInfo.put("selectNwsByMaxUpDate", nwstyp.toLowerCase() + ".selectNwsByMaxUpDate");
        if (nwstyp.equals("NWS")) {
            dsInfo.put("dsNm", MongoDBConfigConstants.TXT_NWS_DB);
        } else if (nwstyp.equals("WCJ")) {
            dsInfo.put("dsNm", MongoDBConfigConstants.TXT_WCJ_DB);
        } else if (nwstyp.equals("YCNC")) {
            dsInfo.put("dsNm", MongoDBConfigConstants.TXT_YCNC_DB);
        }
        return dsInfo;
    }

    /**
     * 删除数据库中已删除的数据
     * @param maxUpDate
     */
    void removeDate(Date maxUpDate){
        logger.info("删除新闻数据开始")
        long start = System.currentTimeMillis()
        String selectIdByMaxUpDate = PropertiesUtil.getProperty(dsInfo.get("selectIdByMaxUpDate"));
        List<Map<String, Object>> sqlList = xmlRawSQLService.queryRawSqlByKey("selectDataSrcByNm",[dsNm:selectIdByMaxUpDate])
        if(sqlList!=null&&sqlList.size()>0){
            //将clob字段转换为string
            String sql = StringUtils.ClobToString(sqlList.get(0).SQL_CLAUSE)
            sql = replaceSql(sql,['${MAXUPDATE}':':MAXUPDATE'])
            List<Map<String, Object>> list = rawSQLService.queryRawSql(sql,[MAXUPDATE:maxUpDate])
            def idsList = mapToDouble(list)
            def nwsList = txtNwsTemplate.find(new Query(Criteria.where("PUB_DT").is(DateUtils.string2Date(DateUtils.date2String(maxUpDate, DateUtils.FORMAT_DATE), DateUtils.FORMAT_DATE))),Nws.class,dsInfo.get("dsNm"));
             nwsList.each {
                 if(!idsList.contains(it.ID)){
                     txtNwsTemplate.remove(new Query(Criteria.where("ID").is(it.ID)),dsInfo.get("dsNm"))
                 }
             }
        }
        long end = System.currentTimeMillis()
        logger.info("删除新闻数据完成，耗时："+(end-start)+"ms")
    }
    /**
     * 保存新闻服务属性
     */
    void savePros(List<Map<String, Object>> props,String dsNm){
        List<NwsPros> list = new ArrayList<NwsPros>()
        props.each {
            list.add(new NwsPros(it.prop_name,it.prop_typ))
        }
        try{
            //判断属性集合是否存在，存在则删除
            if(txtNwsTemplate.collectionExists(dsNm+"_PROS")){
                txtNwsTemplate.dropCollection(dsNm+"_PROS");
            }
            //批量保存属性值
            txtNwsTemplate.insert(list,dsNm+"_PROS");
            logger.info("新闻服务属性创建完成！")
        }catch (err){
            logger.error("新闻服务属性创建错误："+err)
        }
    }

    /**
     * 参数替换
     * @param sql
     * @return
     */
     String replaceSql(String sql,Map<String,String> params){
         params.each {
             while (sql.indexOf(it.key) != -1) {
                 sql = sql.replace(it.key, it.value);
             }
         }
         return sql
     }

    /**
     * 将对象转换为Double
     * @param result
     * @return
     */
    List<Long> mapToDouble(List<Map<String,Object>> result){
        if(result!=null&&result.size()>0){
            List<Double> list = new ArrayList<Double>()
            for (Map<String,Object> map : result){
                list.add((Double)(map.values()).getAt(0))
            }
            return list
        }else{
            return null
        }
    }
}
