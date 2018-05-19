package com.bigdata.datacenter.datasync.service.impl

import com.bigdata.datacenter.datasync.core.data.MongoTemplatePool
import com.bigdata.datacenter.datasync.service.ScanService
import com.bigdata.datacenter.datasync.service.TxtAndRrpStatusService
import com.bigdata.datacenter.datasync.service.XmlRawSQLService
import com.bigdata.datacenter.datasync.utils.DateUtils
import com.bigdata.datacenter.datasync.utils.MongoUtils
import com.bigdata.datacenter.datasync.utils.constants.MongoDBConfigConstants
import com.bigdata.datacenter.datasync.model.mongodb.BltNew
import com.bigdata.datacenter.datasync.model.mongodb.BltNewPros
import com.bigdata.datacenter.metadata.service.RawSQLService
import com.bigdata.datacenter.datasync.utils.StringUtils
import groovy.time.TimeCategory
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.data.domain.Sort
import org.springframework.data.mongodb.core.MongoOperations
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.stereotype.Service
import com.bigdata.datacenter.datasync.utils.ESearchHelper;



/**
 * Created by Dan on 2017/5/27.
 */
@Service(value = "txt-bitNew")
class TxtBltNewServiceImpl implements ScanService{

    private static final Logger logger = Logger.getLogger(TxtBltServiceImpl.class)

    @Autowired
    RawSQLService rawSQLService
    @Autowired
    XmlRawSQLService xmlRawSQLService
    @Autowired
    MongoTemplatePool mongoTemplatePool
    MongoOperations txtBltTemplate

    @Autowired
    TxtAndRrpStatusService txtAndRrpStatusService
    //邮件服务
    @Autowired
    MailService mailService

    private static Map secuidMap = [:]
    //全量跟新
    @Override
    void totalSync() {
        logger.info("公告信息初始化开始")
        long startTime = System.currentTimeMillis();
        try {
            if (secuidMap.isEmpty()) {
                setSecuidMap()
            }
            txtAndRrpStatusService.updateDsStatus(MongoDBConfigConstants.TXT_BLT_DB,
                    TxtAndRrpStatusService.DS_STAS_UPDATE)
            txtBltTemplate = mongoTemplatePool.getByName(MongoDBConfigConstants.TXT_BLT_DB)

            //判断提示信息临时表是否存在，存在则删除
            if(txtBltTemplate.collectionExists(MongoDBConfigConstants.TXT_BLT_DB+"_tmp")){
                txtBltTemplate.dropCollection(MongoDBConfigConstants.TXT_BLT_DB+"_tmp")
            }

            MongoUtils.delCollection(txtBltTemplate, BltNew.class)
            txtBltTemplate.createCollection(BltNew.class)

            def params = [ dsNm : "BD_SYS_SELECTALLBLT_1"]
            def selectAllBltDs = xmlRawSQLService.queryRawSqlByKey("selectDataSrcByNm", params)

            if (selectAllBltDs != null && selectAllBltDs.size() > 0) {

                def dateTimeParam = [:]
                //扫描起始日期
                Date begDt = Date.parse(DateUtils.FORMAT_DATE, "1980-01-01")
                Date endDt = DateUtils.addMonths(begDt, 1);
                dateTimeParam.put("BEGDATE", begDt)
                dateTimeParam.put("ENDDATE", endDt)
                //获取sql的参数
                List<Map<String, Object>> props = xmlRawSQLService.queryRawSqlByKey("selectDopByObjId",
                        [obj_id : selectAllBltDs[0].get("OBJ_ID")])
                // CLOB转成string
                def sql = StringUtils.ClobToString(selectAllBltDs[0].get("sql_clause"))
                sql = replaceSql(sql)

                //逐年扫描
                while (DateUtils.getYearFrDate(begDt) <= DateUtils.getYearFrDate(new Date())) {
                    logger.info("start:" + begDt.format(DateUtils.FORMAT_DATE) + "--end:" +
                            endDt.format(DateUtils.FORMAT_DATE))
                    try{
                        def txtBltNew = new TxtBltNewResultHandler(MongoDBConfigConstants.TXT_BLT_DB+"_tmp",
                                txtBltTemplate, props, secuidMap,mailService)
                        //查询回调并保存数据到mongodb
                        rawSQLService.queryRawSql(sql, dateTimeParam, txtBltNew)
                        List blts = txtBltNew.getBlts();
                        txtBltNew.saveBlt(blts);
                    }catch (e){
                        logger.error("错误信息:start:" + begDt.format(DateUtils.FORMAT_DATE) + "--end:" +
                                endDt.format(DateUtils.FORMAT_DATE)+"时间段时间执行错误："+e)
                    }
//                    use(TimeCategory) {
//                        begDt = begDt + 1.year
//                        endDt = endDt + 1.year
//                    }
                    begDt = DateUtils.addMonths(begDt, 1);
                    endDt = DateUtils.addMonths(endDt, 1);
                    dateTimeParam.put("BEGDATE", begDt)
                    dateTimeParam.put("ENDDATE", endDt)
                }
                //把临时表改名为正常表
                txtBltTemplate.getCollection(MongoDBConfigConstants.TXT_BLT_DB+"_tmp").
                        rename(MongoDBConfigConstants.TXT_BLT_DB,true)
                //新闻服务状态修改为正常，表示更新完成
                txtAndRrpStatusService.updateDsStatus(MongoDBConfigConstants.TXT_BLT_DB,
                        TxtAndRrpStatusService.DS_STAS_NORMAL)
                //保存属性类型
                savePros(props,MongoDBConfigConstants.TXT_BLT_DB,txtBltTemplate);
            }
        }catch (err) {
             logger.error(err)
            txtAndRrpStatusService.updateDsStatus(MongoDBConfigConstants.TXT_BLT_DB,
                    TxtAndRrpStatusService.DS_STAS_ERROR)
            try{
                mailService.sendMail("公告信息初始化失败","【公告信息】初始化失败,错误信息："+err);
            }catch (e){
                logger.error("邮件发送失败，错误信息为："+err)
            }
        } finally {
            try{
                ESearchHelper.closeClient();
            }catch (err){}
		}
        long endTime = System.currentTimeMillis();
        logger.info("公告信息初始化完成,共耗时："+(endTime-startTime)+"ms");
        mailService.sendMail("公告信息完成初始化扫描","【公告信息】初始化完成,共耗时："+(endTime-startTime)+"ms");
    }

    //增量更新
    @Override
    void incrementalSync() {
        logger.info("公告信息增量更新开始")
        long startTime = System.currentTimeMillis();

        Date maxUpDate = null
        try {
            if (secuidMap == null) {
                //setSecuidMap()
            }
            txtAndRrpStatusService.updateDsStatus(MongoDBConfigConstants.
                    TXT_BLT_DB, TxtAndRrpStatusService.DS_STAS_UPDATE)
            txtBltTemplate = mongoTemplatePool.getByName(MongoDBConfigConstants.TXT_BLT_DB)

            def params = [ dsNm : "BD_SYS_SELECTBLTBYUPDTIME_1"]
            def selectBltByUpdTime = xmlRawSQLService.queryRawSqlByKey("selectDataSrcByNm", params)
            List<Map<String, Object>> props = xmlRawSQLService.queryRawSqlByKey("selectDopByObjId",
                        [obj_id : selectBltByUpdTime[0].get("obj_Id")])
            Query query=new Query();
            query.with(new Sort(new Sort.Order(Sort.Direction.DESC,"UPD_TIME")));
            BltNew blt = txtBltTemplate.findOne(query,BltNew.class,MongoDBConfigConstants.TXT_BLT_DB)
            if (blt != null) {
                maxUpDate = blt.getUPD_TIME()
            }
            if (maxUpDate != null && maxUpDate != "") {
                def sql = StringUtils.ClobToString(selectBltByUpdTime[0].get("sql_clause"))
                sql = replaceSql(sql)
                def txtBltNew = new TxtBltNewResultHandler(MongoDBConfigConstants.TXT_BLT_DB, txtBltTemplate,
                            props, secuidMap,mailService)
                def dateTimeParam = [:]
                dateTimeParam.put("MAXUPDATE", maxUpDate)
                rawSQLService.queryRawSql( sql , dateTimeParam , txtBltNew)
                List blts = txtBltNew.getBlts();
                txtBltNew.saveBlt(blts);
            }
        }catch (err) {
            logger.error(err)
            txtAndRrpStatusService.updateDsStatus(MongoDBConfigConstants.TXT_BLT_DB,TxtAndRrpStatusService.DS_STAS_ERROR)
            mailService.sendMail("公告信息增增量更新失败","【公告信息】增量更新失败,错误信息："+err);
        } finally {
		    ESearchHelper.closeClient();
		}
        long endTime = System.currentTimeMillis();
        logger.info("公告信息增量更新完成,共耗时："+(endTime-startTime)+"ms");
    }

    private def setSecuidMap() {
        long start = System.currentTimeMillis()
        logger.info("getSecuidMap" + ":" + "start")
        secuidMap.clear()

        def params = [dsNm : "BD_SYS_SELECTSECUIDLST_1"]
        def list = xmlRawSQLService.queryRawSqlByKey("selectDataSrcByNm",params)

         //将clob类型的sql语句转换成string
        def sql = StringUtils.ClobToString(list[0].get("sql_clause"))

        //执行sql语句
        def secuidMapLst1 = rawSQLService.queryRawSql(sql)

        for (item in secuidMapLst1) {
            if (item == null) {
                break
            }

            int secu_id = item.SECU_ID as int
            int com_id = item.COM_ID as int

            if (secuidMap.keySet().contains(com_id)) {
                List secuLst = secuidMap.get(com_id)

                if (!secuLst.contains(secu_id)) {
                    secuLst.add(secu_id)
                }
            } else {
                secuidMap.put(com_id, [secu_id])
            }

        }

        long end = System.currentTimeMillis()
        logger.info("getSecuidMap" + ":" + (end-start) + "ms")
    }
    //删除数据库中已经删除的数据
    void removeData (){
        logger.info("删除公告数据开始")
        long start = System.currentTimeMillis()
        //获取最新数据时间
        Date maxUpDate = null
        Query query=new Query();
        query.with(new Sort(new Sort.Order(Sort.Direction.DESC,"UPD_TIME")));
        BltNew cursor = txtBltTemplate.findOne(query, BltNew.class, MongoDBConfigConstants.TXT_BLT_DB)

        if (cursor != null) {
            maxUpDate = cursor.getUPD_TIME()
        }
        if (maxUpDate != null || maxUpDate != "") {
            def dateTimeParam = [:]
            def params = [ dsNm : "BD_SYS_SELECTBLTBYUPDTIME_1"]

            def selectBltIdByMaxUpDate = xmlRawSQLService.queryRawSqlByKey("selectDataSrcByNm", params)
            def sql = StringUtils.ClobToString(selectBltIdByMaxUpDate[0].get("sql_clause"))
            sql = replaceSql(sql)
            dateTimeParam.put("MAXUPDATE", maxUpDate)
            List ids = txtBltTemplate.find(sql, dateTimeParam, Double.class)
            Date condDate = DateUtils.string2Date(DateUtils.date2String(maxUpDate,
                    DateUtils.FORMAT_DATE), DateUtils.FORMAT_DATE)
            List<BltNew> cursors = txtBltTemplate.find(new Query(Criteria.where("UPD_TIME").gte(condDate),
                    BltNew.class, MongoDBConfigConstants.TXT_BLT_DB))

            if (cursors != null && cursors != "") {
                for (item in cursors) {
                    def id = item.getID()
                    if (!ids.contains(id)) {
                        //删除数据
                        txtBltTemplate.remove(item)
                    }
                }
            }
            long end = System.currentTimeMillis()
            logger.info("删除公告数据完成，耗时："+(end-start)+"ms")
        }
    }

    //保存数据源属性
    void savePros(List<Map<String, Object>> props, String dsNm, MongoOperations txtBltTemplate){
        def list = []
        props.each {
            list.add(new BltNewPros(it.prop_name,it.prop_typ))
        }
        try{
            //判断属性集合是否存在，存在则删除
            if(txtBltTemplate.collectionExists(dsNm+"_PROS")){
                txtBltTemplate.dropCollection(dsNm+"_PROS");
            }
            //批量保存属性值
            txtBltTemplate.insert(list,dsNm+"_PROS");
            logger.info("公告属性创建完成！")
        }catch (err){
            logger.error("公告属性创建错误："+err)
        }
    }

    String replaceSql(String sql) {
         while (sql.indexOf('${BEGDATE}') != -1
                 || sql.indexOf('${ENDDATE}') != -1
                 || sql.indexOf('${MAXUPDATE}') != -1) {
             if (sql.indexOf('${BEGDATE}') != -1) {
                 sql = sql.replace("\${BEGDATE}", ":BEGDATE");
             } else if (sql.indexOf('${ENDDATE}') != -1) {
                 sql = sql.replace("\${ENDDATE}", ":ENDDATE");
             }
             else if (sql.indexOf("\${MAXUPDATE}") != -1) {
                 sql = sql.replace("\${MAXUPDATE}", ":MAXUPDATE")
             }
         }
         return sql
    }
}
