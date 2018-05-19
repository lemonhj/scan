package com.bigdata.datacenter.datasync.service.impl

import com.bigdata.datacenter.datasync.core.data.MongoTemplatePool
import com.bigdata.datacenter.datasync.model.mongodb.DsStatus
import com.bigdata.datacenter.datasync.model.mongodb.ParamObj
import com.bigdata.datacenter.datasync.model.mongodb.SubjectStatus
import com.bigdata.datacenter.datasync.service.ScanService
import com.bigdata.datacenter.datasync.service.SubjectStatusService
import com.bigdata.datacenter.datasync.service.XmlRawSQLService
import com.bigdata.datacenter.datasync.utils.DateUtils
import com.bigdata.datacenter.datasync.utils.PropertiesUtil
import com.bigdata.datacenter.datasync.utils.constants.MongoDBConfigConstants
import com.bigdata.datacenter.metadata.service.RawSQLService
import com.bigdata.datacenter.metadata.service.ResultHandler;
import com.bigdata.datacenter.metadata.utils.EntityUtil
import com.bigdata.datacenter.metadata.utils.StringUtils
import com.fasterxml.jackson.databind.ObjectMapper
import com.mongodb.BasicDBObject

import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.data.mongodb.core.MongoOperations
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.stereotype.Service

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern

import com.bigdata.datacenter.metadata.data.EbeanServerPool
import com.avaje.ebean.EbeanServer
import com.avaje.ebean.SqlQuery
import java.util.function.Consumer
import com.avaje.ebean.SqlRow

/**
 * Created by Dan on 2017/6/20.
 */
@Service(value = "all-index-page")
class AllIndexServiceImpl2Page implements ScanService {

    private static final Logger logger = Logger.getLogger(AllIndexServiceImpl2Page.class)
    private static final int IDX_STAS_NORMAL = 0
    private static final int IDX_STAS_UPDATE = 1
    private static final int IDX_STAS_ERROR = 2
    static final String KEY = "Key"
    static final String Value = "Value"
    static final String OPER_TYPE_UPDATE = "update"
    static final String OPER_TYPE_SAVE = "save"
    static List dsNms = null
    static List m_IncludeDS = null
    static List m_ExcludeDS = null
    def private BEG_TIME = "beginDateTime"
    def private END_TIME = "endDateTime"
    @Autowired
    RawSQLService rawSQLService
    @Autowired
    XmlRawSQLService xmlRawSQLService
    @Autowired
    @Qualifier(value = "subjectStatusServiceImpl")
    SubjectStatusService subjectStatusService
    @Autowired
    MongoTemplatePool mongoTemplatePool
    MongoOperations allIndexTemplate
    AllIndexFrMongoResultHandler2Page resultHandler = new AllIndexFrMongoResultHandler2Page()
	@Autowired
	EbeanServerPool ebeanServerPool;
    //全量更新
    @Override
    void totalSync() {
        logger.info("指标数据初始化开始")
        long startTime = System.currentTimeMillis();
        allIndexTemplate = mongoTemplatePool.getByName(MongoDBConfigConstants.INDEX_ALL_DB)
        //创建索引
        allIndexTemplate.getCollection(MongoDBConfigConstants.INDEX_ALL_DB).createIndex(new BasicDBObject(KEY, -1))
        List<Map<String,Object>> indexLst = xmlRawSQLService.queryRawSqlByKey("selectIndexAll")
        //针对部分源保存
        if (dsNms != null && dsNms.size() > 0) {
            for (int i = 0; i < indexLst.size(); i++) {
                if (!dsNms.contains(indexLst.get(i).get("ds_eng_name"))) {
                    indexLst.remove(i)
                    i--
                }
            }
        } else {
            //全扫描则创建下次更新扫描开始时间
            PropertiesUtil.writeProperties(PropertiesUtil.LAST_SCAN_TIME_FILE,PropertiesUtil.LAST_SCAN_TIME, DateUtils.date2String(new Date(),DateUtils.FORMAT_DATETIME))
        }
        Map<String,Map<String,Date>> dsTimeMap = [:]
        Map<String,String> timeMap = getScanTime()
        // 过滤指标结果集
        execFirInsLst(indexLst, OPER_TYPE_SAVE, dsTimeMap, timeMap)

        long endTime = System.currentTimeMillis();
        logger.info("指标数据初始化结束,耗时"+(endTime-startTime)+"ms")
    }

    //增量跟新
    @Override
    void incrementalSync() {
        logger.info("指标数据增量更新开始")
        long startTime = System.currentTimeMillis();

        allIndexTemplate = mongoTemplatePool.getByName(MongoDBConfigConstants.INDEX_ALL_DB)
        Map timeMap = getScanTime()
        List<Map<String,Object>> dataSrcs;
        Map<String, Map<String, Date>> dsTimeMap = new HashMap<String, Map<String, Date>>();
        Map<String, Map<String, Object>> tblTimeMap = new HashMap<String, Map<String, Object>>();
        if (PropertiesUtil.getProperty("idx.fr.sub") == "true") {
            //从专题的mongoDB更新
            dataSrcs = getChgTbl(MongoDBConfigConstants.IDX_TBL_UPD_TIME, MongoDBConfigConstants.IDX_STATUS_DB,
                    timeMap, dsTimeMap, tblTimeMap)
        }else {
            //从数据库更新
            dataSrcs = getChgTbls(MongoDBConfigConstants.IDX_TBL_UPD_TIME, timeMap, tblTimeMap, dsTimeMap)
        }
        //过滤不需要处理的数据源
        dataSrcs = getNeedScanList(dataSrcs)
        //保存全部的数据源名称
        List<String> dsNmLst = []
        dataSrcs.each{
            dsNmLst.add(it.DS_ENG_NAME)
        }
        // 已有的指标列表
        List indexLst = xmlRawSQLService.queryRawSqlByKey("selectIndexAll")
        // 剔除未变化的指标
        for (int i = 0; i < indexLst.size(); i++) {
            if (!dsNmLst.contains(indexLst.get(i).get("ds_eng_name"))) {
                indexLst.remove(indexLst.get(i))
                i--
            }
        }
        execFirInsLst(indexLst, OPER_TYPE_UPDATE, dsTimeMap, timeMap)
        long endTime = System.currentTimeMillis();
        logger.info("指标数据增量更新结束,耗时"+(endTime-startTime)+"ms")
    }

    //指标源属性变更后更新
    public void UpdateIdxProChgToMgDB() {
        if (PropertiesUtil.getProperty("index.prochg") == "false"){
            return
        }
        allIndexTemplate = mongoTemplatePool.getByName(MongoDBConfigConstants.INDEX_ALL_DB)
        String lastIdxChgStartDate = PropertiesUtil.getProperty(PropertiesUtil.LAST_SCAN_TIME_FILE,PropertiesUtil.LAST_IDX_CHG_DATE)
        String lastIdxChgEndDate = DateUtils.date2String(new Date(), DateUtils.FORMAT_DATE)

        //指标列表
        List indexLst = xmlRawSQLService.queryRawSqlByKey("selectIdxByUdpTime",
                  ["beginDateTime":lastIdxChgStartDate,"endDateTime":lastIdxChgEndDate])
        Map dsTimeMap = [:]
        Map timeMap = [:]
        // 过滤指标结果集
        execFirInsLst(indexLst, OPER_TYPE_SAVE, dsTimeMap, timeMap)
        // 写入下次更新参数时间
        //全扫描则创建下次更新扫描开始时间
        PropertiesUtil.writeProperties(PropertiesUtil.LAST_SCAN_TIME_FILE,PropertiesUtil.LAST_IDX_CHG_DATE, lastIdxChgEndDate)
    }

    //更新宏观指标
    public void updateMarcoIndexToMgDB() {
        logger.info("宏观指标更新开始")
        long startTime = System.currentTimeMillis();
        MongoOperations idMarcoTemplate = mongoTemplatePool.getByName(MongoDBConfigConstants.IDX_MARCO)
        List<Map<String,Object>> dataSrcs = xmlRawSQLService.queryRawSqlByKey("selectMarcoDataSrc")
        for (dds in dataSrcs) {
           def dsName = dds.DS_ENG_NAME
           try{
               logger.debug("宏观指标【" + dsName + "】开始")
               long start = System.currentTimeMillis();
               List props = xmlRawSQLService.queryRawSqlByKey("selectDopByObjId", [obj_id : dds.OBJ_ID])
               Date oldUpdTime = getMacroMaxUpdTime(idMarcoTemplate, dsName)

               AllIndexMacroDsResultHandler dsRow = new AllIndexMacroDsResultHandler(idMarcoTemplate,props, oldUpdTime,dsName)
			   String sql = StringUtils.ClobToString(dds.SQL_CLAUSE) + " where UPD_TIME >= TO_DATE('" + DateUtils.date2String(oldUpdTime, DateUtils.FORMAT_DATETIME) + "', " +"'YYYY-MM-DD HH24:MI:SS')" + " order by UPD_TIME asc";
			   executeHandler(dds.db_name,dsRow,sql,null,dsName);
			   
               /*rawSQLService.queryRawSql(StringUtils.ClobToString(dds.SQL_CLAUSE) + " where UPD_TIME >= TO_DATE('" +
                       DateUtils.date2String(oldUpdTime, DateUtils.FORMAT_DATETIME) + "', " +
                       "'YYYY-MM-DD HH24:MI:SS')" + " order by UPD_TIME asc", dsRow)*/
               long end = System.currentTimeMillis();
               logger.debug("宏观指标【" + dsName + "】结束：耗时："+(end-start)+"ms")
           }catch(err){
               logger.error("宏观指标【" + dsName + "】更新失败："+err)
           }
        }
        long endTime = System.currentTimeMillis();
        logger.info("宏观指标更新结束，耗时："+(endTime - startTime)+"ms");
    }

    //删除不存在的指标数据或者已修改参数后残留数据
    public void delDirtyDataToMgDB() {
        logger.info("定时删除无效指标开始")
        long startTime = System.currentTimeMillis();
        //最后一次扫描的指标ID
        int lastScanIdx = 1
        int initalDelIdxCnt = 10
        Map idxMap = [:]
        List indexIdxLst = []
        allIndexTemplate = mongoTemplatePool.getByName(MongoDBConfigConstants.INDEX_ALL_DB)
        List indexLst = xmlRawSQLService.queryRawSql("selectIndexAll")
        for (int i = 0; i < indexLst.size(); i++) {
            int _idx = indexLst.get(i).get("IDX_ID").intValue()
            if (!indexIdxLst.contains(_idx)) {
                indexIdxLst.add(_idx)
                idxMap.put(_idx, indexLst.get(i))
            }
        }
        Collections.sort(indexIdxLst)
        int maxScanIdx = indexIdxLst.get(indexIdxLst.size() - 1)
        String lastScnIdxStr = PropertiesUtil.getProperty(PropertiesUtil.LAST_SCAN_TIME_FILE,PropertiesUtil.LAST_SCAN_IDX)
        if (lastScnIdxStr != null) {
            lastScanIdx = Integer.valueOf(lastScnIdxStr) + 1
        }
        int cnt = 1
        int delCount = 0
        while (true) {
            if (lastScanIdx > maxScanIdx) {
                lastScanIdx = 1
            }
            Pattern pattern = Pattern.compile("^" + lastScanIdx + "_.*\$")
            List cur = allIndexTemplate.find(new Query(Criteria.where("Key").is(pattern), Map.class,MongoDBConfigConstants.INDEX_ALL_DB))
            logger.info("scan idx  id-" + lastScanIdx + ": start")
            if (!indexIdxLst.contains(lastScanIdx)) {
                for (var in cur) {
                    allIndexTemplate.remove(var, MongoDBConfigConstants.INDEX_ALL_DB)
                    delCount++
                }
                logger.debug("delete dirty index " + pattern + ":" + delCount);
            } else {
                String idxParam = idxMap.get(lastScanIdx).get("IDX_PARAM")
                // 带有参数的情况下
                ObjectMapper mapper = new ObjectMapper();
                // 指标参数
                ParamObj[] result = mapper.readValue(idxParam,ParamObj[].class)
                for (var in cur) {
                    String keyStr = var.get("Key")
                    // IDX_参数 = 参数 + 1
                    if (keyStr.split("_").length != result.length + 1) {
                        allIndexTemplate.remove(var, MongoDBConfigConstants.INDEX_ALL_DB)
                        delCount++;
                    }
                }
                logger.debug("delete dirty index " + pattern + ":" + delCount);
            }
            delCount = 0
            // 全扫描则创建下次更新扫描开始时间
            PropertiesUtil.writeProperties(PropertiesUtil.LAST_SCAN_TIME_FILE,PropertiesUtil.LAST_SCAN_IDX, String.valueOf(lastScanIdx))
            logger.info("scan idx  id-" + lastScanIdx + ": end")
            cnt++
            lastScanIdx++
            // 删除5个指标停止
            if (cnt > initalDelIdxCnt) {
                break
            }
        }
        long endTime = System.currentTimeMillis()
        logger.info("定时删除无效指标结束，耗时："+(endTime-startTime)+"ms")
    }
    /**
     * 获取宏观指标更新开始时间
     * @param idMarcoTemplate
     * @param dsNm
     * @return
     */
    private Date getMacroMaxUpdTime(MongoOperations idMarcoTemplate, String dsNm) {
		Date oldUpdTime = DateUtils.string2Date("1900-01-01", DateUtils.FORMAT_DATE)
		if (idMarcoTemplate.collectionExists(MongoDBConfigConstants.IDX_MARCO_UPD)) {
			Map old = idMarcoTemplate.findOne(new Query(Criteria.where("DS_NAME").is(dsNm)),
                    Map.class, MongoDBConfigConstants.IDX_MARCO_UPD)
			if (old != null) {
				oldUpdTime = (Date)old.get("MAX_UPD_TIME")
			}
		}
		return oldUpdTime
	}

    /**
     * 获取需要更新的数据源信息
     * @param colNm
     * @param dateTimeMap
     * @param tblTimeMap
     * @param dsTimeMap
     * @return
     */
    List<Map<String,Object>> getChgTbls(String colNm, Map<String, String> dateTimeMap,
                                        Map<String, Map<String, Object>> tblTimeMap,
                                        Map<String, HashMap<String, Date>> dsTimeMap){
        List<Map<String,Object>> tbls = xmlRawSQLService.queryRawSqlByKey("selectAllTbl")
        MongoTemplate col = mongoTemplatePool.getByName(colNm)
        //保存数据源名称
        List<String> needTbls = new ArrayList<String>();
        //保存需要更新的数据源信息
        List<Map<String,Object>> dsLst = new ArrayList<Map<String,Object>>();
        //去除重复的数据源名称
        List<String> dsNms = new ArrayList<String>();
        tbls.each {
            if (!needTbls.contains(it.TBL_NAME)) {
                needTbls.add(it.TBL_NAME);
                try {
                    String sql = 'select max(Upd_Time) UPD_TIME from ' + it.TBL_NAME
                    List<Map<String, Object>> result = rawSQLService.queryRawSql(sql)
                    Date maxTblDate = null;
                    if (result != null && result.size() > 0) {
                        maxTblDate = (Date) result.get(0).UPD_TIME
                    }
                    Date beginDate = DateUtils.string2Date(dateTimeMap.beginDateTime, DateUtils.FORMAT_DATETIME);
                    Map<String, Object> lastObj = col.findOne(new Query(Criteria.where("TBL_NAME").is(it.TBL_NAME)), Map.class)
                    Date lastTblUpdTime;
                    boolean noFlg = false;
                    // 保存各表的最新更新时间
                    if (null == lastObj) {
                        noFlg = true;
                        lastTblUpdTime = beginDate;
                        tblTimeMap.put(it.tbl_name, ["TBL_NAME": it.TBL_NAME, "UPD_TIME": maxTblDate]);
                    } else {
                        lastTblUpdTime = (Date) lastObj.UPD_TIME;
                        lastObj.put("UPD_TIME", maxTblDate);
                        tblTimeMap.put(it.TBL_NAME, lastObj);
                    }
                    // 判断最新数据时间是否在扫描时间周期内
                    if (noFlg || maxTblDate.compareTo(lastTblUpdTime) > 0) {
                        List<Map<String, Object>> dataSrcs = xmlRawSQLService.queryRawSqlByKey("selectDataSrcByTbl", [TBL_NAME: it.TBL_NAME])

                        // 获取每个源上次更新时间及设置本次更新时间
                        for (Map<String, Object> dds : dataSrcs) {
                            String dsNm = dds.DS_ENG_NAME;
                            SubjectStatus dsStatus = subjectStatusService.getSubStatusByDsName(dsNm)
                            Date lastUpdTimeStr = null
                            if(dsStatus!=null){
                                lastUpdTimeStr = dsStatus.LAST_UPD_TIME
                            }
                            // 源的上次更新时间为空则使用表的上次更新时间
                            if (null == lastUpdTimeStr) {
                                lastUpdTimeStr = lastTblUpdTime
                            }
                            // 源的更新时间为空则使用表的更新填充
                            if (dsTimeMap.get(dsNm) == null) {
                                dsTimeMap.put(dsNm, ["beginDateTime": lastUpdTimeStr, "endDateTime": maxTblDate])
                            }
                            // 给每个源填入更新开始时间和更新结束时间
                            // 开始扫描时间
                            if (dsTimeMap.get(dsNm).get("beginDateTime").compareTo(lastTblUpdTime) > 0) {
                                dsTimeMap.get(dsNm).put("beginDateTime", lastTblUpdTime)
                            }
                            // 结束扫描时间
                            if (dsTimeMap.get(dsNm).get("endDateTime").compareTo(maxTblDate) < 0) {
                                dsTimeMap.get(dsNm).put("endDateTime", maxTblDate)
                            }
                            // 加入到数据源数组
                            if (!dsNms.contains(dsNm)) {
                                dsNms.add(dsNm)
                                dsLst.add(dds)
                            }
                        }
                    }
                }catch (err){
                    logger.error(err)
                }
            }
        }
        return dsLst
    }

    /**
     * 从mongo获取需要更新的数据源信息
     * @param colNm
     * @param dateTimeMap
     * @param tblTimeMap
     * @param dsTimeMap
     * @return
     */
    public List getChgTbl(String colName,String idxStatusDb,Map dateTimeMap,Map dsTimeMap,Map tblTimeMap ) {
        List dsLst = []

        MongoOperations idxUpdTimeTemplate = mongoTemplatePool.getByName(colName)
        //获取数据源的更新时间
        MongoOperations subUpdTimeTemplate = mongoTemplatePool.getByName(MongoDBConfigConstants.SUB_TBL_UPD_TIME)
        List<Map<String,Object>> dsTbls = subUpdTimeTemplate.findAll(Map.class, MongoDBConfigConstants.SUB_TBL_UPD_TIME)

        for (var in dsTbls) {
            def tblNm = var.TBL_NAME
            def maxDate = var.UPD_TIME
            def beginDate = DateUtils.string2Date(dateTimeMap.get(BEG_TIME), DateUtils.FORMAT_DATETIME)
            Map lastObj = idxUpdTimeTemplate.findOne(new Query(Criteria.where("TBL_NAME").is(tblNm)), Map.class, colName)
            Date lastUpdTime;
            boolean noFlg = false
            if (null == lastObj) {
                noFlg = true
                lastUpdTime = beginDate
                lastObj.clear()
                lastObj.put("TBL_NAME", tblNm)
                // 保存表最新更新时间
                lastObj.put("UPD_TIME", maxDate)
                tblTimeMap.put(tblNm, lastObj)
            } else {
                lastUpdTime = (Date) lastObj.UPD_TIME
                lastObj.put("UPD_TIME", maxDate)
                tblTimeMap.put(tblNm, lastObj)
            }
            // 判断最新数据时间是否在扫描时间周期内
            if (noFlg || maxDate.compareTo(lastUpdTime) > 0) {
                List dataSrcs = xmlRawSQLService.queryRawSqlByKey("selectDataSrcByTbl", [TBL_NAME: tblNm])
                // 获取每个源上次更新时间及设置本次更新时间
                for (Map<String, Object> dds : dataSrcs) {
                    String dsNm = dds.DS_ENG_NAME
                    Date lastUpdTimeStr = readDsUpdTimeFrMg(dsNm, idxStatusDb)
                    // 源的上次更新时间为空则使用表的上次更新时间
                    if (null == lastUpdTimeStr) {
                        lastUpdTimeStr = lastUpdTime;
                    }
                    // 源的更新时间为空则使用表的更新填充
                    if (dsTimeMap.get(dsNm) == null) {
                        Map timeMap = [:]
                        timeMap.put(BEG_TIME, lastUpdTimeStr)
                        timeMap.put(END_TIME, maxDate)
                        dsTimeMap.put(dsNm, timeMap)
                    }
                    // 给每个源填入更新开始时间和更新结束时间
                    // 开始扫描时间
                    if (dsTimeMap.get(dsNm).get(BEG_TIME).compareTo(lastUpdTime) > 0) {
                        dsTimeMap.get(dsNm).put(BEG_TIME, lastUpdTime)
                    }
                    // 结束扫描时间
                    if (dsTimeMap.get(dsNm).get(END_TIME).compareTo(maxDate) < 0) {
                        dsTimeMap.get(dsNm).put(END_TIME, maxDate)
                    }
                    // 加入到数据源数组
                    if (!dsNms.contains(dsNm)) {
                        dsNms.add(dsNm)
                        dsLst.add(dds)
                    }
                }
            }

        }
		return dsLst
    }

    /**
     * 获取数据源的最后更新时间
     * @param dsNm
     * @param dbNm
     * @return
     */
    public Date readDsUpdTimeFrMg(String dsNm,String dbNm) {
		Date updTime = null
		MongoOperations dsColTemplate = mongoTemplatePool.getByName(dbNm)
		DsStatus dsStatus = dsColTemplate.findOne(new Query(Criteria.where("ds_name").is(dsNm)), Map.class, dbNm)
		if (null !=dsStatus ) {
			updTime = (Date)dsStatus.get("last_upd_time")
		}
		return updTime
	}

    /**
     * 根据数据源过滤指标
     * @param indexLst
     * @param operType
     * @param dsTimeMap
     * @param timeMap
     */
    void execFirInsLst(List<Map<String,Object>> indexLst, String operType, Map<String,Map<String,Date>> dsTimeMap, Map<String,String> timeMap) {
        //处理过的指标id存储的位置
        List indexNmLst = []
        //保存数据源对应的指标
        List<Map<String,Object>> wtExeInsLst = []
        def dsEngName = ""
        //创建线程池
        ThreadPoolExecutor producerPool = new ThreadPoolExecutor(20, 20, 1,TimeUnit.SECONDS, new LinkedBlockingQueue(),
                new ThreadPoolExecutor.CallerRunsPolicy())
        //循环遍历指标进行数据处理
        for (indexObj in indexLst) {
            if (indexNmLst.contains(indexObj.IDX_ID)) {
                continue
            }
            indexNmLst.add(indexObj.IDX_ID)
            // 记录上次的数据源名称,不同则一组指标更新
            if (dsEngName != ""  && dsEngName != indexObj.DS_ENG_NAME) {
                Map<String,Date> dsMap = dsTimeMap.get(dsEngName)
                if (dsMap == null) {
                    dsMap = [:]
                    dsMap.put(BEG_TIME, DateUtils.string2Date(timeMap.get(BEG_TIME), DateUtils.FORMAT_DATETIME))
                    dsMap.put(END_TIME, DateUtils.string2Date(timeMap.get(END_TIME), DateUtils.FORMAT_DATETIME))
                    dsTimeMap.put(dsEngName, dsMap)
                }
                producerPool.execute(new ThreadPoolTaskM(wtExeInsLst, operType,dsTimeMap.get(dsEngName), PropertiesUtil.getProperty("idx.fr.sub")));
                wtExeInsLst = []
            }
            wtExeInsLst.add(indexObj)
            dsEngName = indexObj.DS_ENG_NAME
        }
        //循环外再做一次
        if (wtExeInsLst.size() != 0) {
            Map<String,Date> dsMap = dsTimeMap.get(dsEngName)

            if (dsMap == null) {
                dsMap = [:]
                dsMap.put(BEG_TIME, DateUtils.string2Date(timeMap.get(BEG_TIME), DateUtils.FORMAT_DATETIME))
                dsMap.put(END_TIME, DateUtils.string2Date(timeMap.get(END_TIME), DateUtils.FORMAT_DATETIME))
                dsTimeMap.put(dsEngName, dsMap)
            }
            producerPool.execute(new ThreadPoolTaskM(wtExeInsLst, operType,dsTimeMap.get(dsEngName), PropertiesUtil.getProperty("idx.fr.sub")))
        }

        if (producerPool != null) {
            producerPool.shutdown()
            while (!producerPool.awaitTermination(1, TimeUnit.SECONDS)) {
            }
            // 关闭线程池
            logger.info(operType + "IndexToMgDB" + " CompletedTaskCount:" + producerPool.getCompletedTaskCount());
        }
    }

    /**
     * 多线程保存数据至mongoDB
     * */
    private class ThreadPoolTaskM implements Runnable, Serializable {
        private List<Map<String,Object>> wtExeInsLst
        private String operType
        private Map<String,Date> timeMap
        private String idxFrSubFlg

        public ThreadPoolTaskM(List<Map<String,Object>> wtExeInsLst, String operType, Map<String,Date> timeMap,String idxFrSubFlg) {
            this.wtExeInsLst = wtExeInsLst
            this.operType = operType
            this.timeMap = timeMap
            this.idxFrSubFlg = idxFrSubFlg
        }

        @Override
        public void run() {
            try {
                if(operType == OPER_TYPE_SAVE){
                    logger.info("指标数据源【"+wtExeInsLst.get(0).DS_ENG_NAME+"】初始化开始");
                }else {
                    logger.info("指标数据源【"+wtExeInsLst.get(0).DS_ENG_NAME+"】更新开始");
                }
                long start = System.currentTimeMillis();
                // 是否从mongoDB更新
                if (idxFrSubFlg == "true") {
                    execAllIdxFormMongo(wtExeInsLst, operType, timeMap);
                } else {
                    execAllIdxFormDB(wtExeInsLst, operType, timeMap);
                }
                long end = System.currentTimeMillis();
                if(operType == OPER_TYPE_SAVE){
                    logger.info("指标数据源【"+wtExeInsLst.get(0).DS_ENG_NAME+"】初始化结束，耗时："+(end-start)+"ms");
                }else {
                    logger.info("指标数据源【"+wtExeInsLst.get(0).DS_ENG_NAME+"】更新结束，耗时："+(end-start)+"ms");
                }
            } catch (Exception e) {
                updateIdxStatus(wtExeInsLst.get(0).DS_ENG_NAME,(Long)wtExeInsLst.get(0).DS_OBJ_ID,(Long)wtExeInsLst.get(0).DS_TYP, IDX_STAS_ERROR)
                if(operType == OPER_TYPE_SAVE){
                    logger.error("指标数据源【"+wtExeInsLst.get(0).DS_ENG_NAME+"】初始化失败："+e.toString());
                }else {
                    logger.error("指标数据源【"+wtExeInsLst.get(0).DS_ENG_NAME+"】更新失败："+e.toString());
                }
            }
        }
    }

    /**
     * 从mongo更新指标数据
     * @param wtExeInsLst
     * @param operType
     * @param getScanTime
     */
    private void execAllIdxFormMongo(List<Map<String,Object>> wtExeInsLst, String operType, Map<String,Date> getScanTime) {
        List<Map<String,Object>> wtExeInsLstTmp = new ArrayList<Map<String,Object>>();
        wtExeInsLstTmp.addAll(wtExeInsLst)
        if (wtExeInsLstTmp.size() == 0) {
			return
		}
        Map<String,Object> indexObjOne = wtExeInsLstTmp.get(0)
        String dsEngName = indexObjOne.DS_ENG_NAME

        //获取数据源属性
        List<Map<String,Object>> props = xmlRawSQLService.queryRawSqlByKey("selectDopByObjId", [obj_id: indexObjOne.DS_OBJ_ID])
        //更新指标状态
        updateIdxStatus(dsEngName,(Long)indexObjOne.DS_OBJ_ID, (Long)indexObjOne.DS_TYP,IDX_STAS_UPDATE)
        //获取指标参数列表
        ObjectMapper mapper = new ObjectMapper()
        ParamObj[] result =mapper.readValue(indexObjOne.IDX_PARAM, ParamObj[].class)
        Map<String,Map<String,Object>> propsMap = getProsMap(props)
        //静态数据源
        if (indexObjOne.get("DS_TYP") == 2 || (indexObjOne.get("DS_TYP") == 6 && (indexObjOne.get("DS_PARAM") == null
                || indexObjOne.DS_PARAM == ""))) {
            resultHandler.mongoTemplatePool = mongoTemplatePool
            resultHandler.allIndexTemplate = allIndexTemplate
            resultHandler.saveIdxFormMongo(wtExeInsLstTmp, propsMap,dsEngName, result)
        }
        //静态分段
        else if (indexObjOne.get("DS_TYP") == 3)  {
            ObjectMapper dsMapper = new ObjectMapper();
            ParamObj[] dsResult =dsMapper.readValue(indexObjOne.PARAM_DFT_VAL, ParamObj[].class)
            // 按照参数名称字母排序组成mongoDB集合名称
            Collections.sort(Arrays.asList(result))
            // 设置各参数类型
            Map<String, Long> paramMap = new HashMap<String, Long>();
            dsResult.each{
                //获取各自参数的类型
                List<Map<String,Object>> paramTypes = xmlRawSQLService.queryRawSqlByKey("selectParamType", [param_code : it.type])
                if(paramTypes!=null&&paramTypes.size()>0){
                    paramMap.put(it.name, paramTypes.get(0).param_typ);
                }
            }

            AllIndexFrMongoResultHandler2Page fullParamRowFromMongo = new AllIndexFrMongoResultHandler2Page(indexObjOne, result,
                    paramMap, wtExeInsLstTmp, propsMap, allIndexTemplate, mongoTemplatePool,dsResult)
            if (operType == OPER_TYPE_SAVE) {
                String sql = indexObjOne.DS_PARAM_FULL
//                rawSQLService.queryRawSql(sql, fullParamRowFromMongo)
                executeHandler(wtExeInsLst.get(0).get("db_name"),fullParamRowFromMongo,sql,null,indexObjOne.DS_ENG_NAME);
            }else if (operType == OPER_TYPE_UPDATE) {
                String beginDateTime = DateUtils.date2String(getScanTime.get(BEG_TIME),DateUtils.FORMAT_DATETIME);
                String endDateTime = DateUtils.date2String(getScanTime.get(END_TIME),DateUtils.FORMAT_DATETIME);
                // 获取更新参数的sql
                String sql = StringUtils.ClobToString(indexObjOne.DS_PARAM).replace("\${BGN_TIME}", beginDateTime).
                        replace("\${END_TIME}", endDateTime)
//                rawSQLService.queryRawSql(sql, fullParamRowFromMongo)
                executeHandler(wtExeInsLst.get(0).get("db_name"),fullParamRowFromMongo,sql,null,indexObjOne.DS_ENG_NAME);
            }
        } else if (indexObjOne.get("DS_TYP") == 7) {
            // 解析默认参数
            ObjectMapper dsMapper = new ObjectMapper()
            ParamObj[] incDsResult =dsMapper.readValue(StringUtils.ClobToString(indexObjOne.UPD_KEY), ParamObj[].class)
            // 按照参数名称字母排序组成mongoDB集合名称
            Collections.sort(Arrays.asList(result))
            // 设置各参数类型
            Map<String, Long> paramMap = new HashMap<String, Long>();
            incDsResult.each{
                //获取各自参数的类型
                List<Map<String,Object>> paramTypes = xmlRawSQLService.queryRawSqlByKey("selectParamType", [param_code : it.type])
                if(paramTypes!=null&&paramTypes.size()>0){
                    paramMap.put(it.name, paramTypes.get(0).param_typ);
                }
            }
            AllIndexFrMongoResultHandler2Page fullParamRowFromMongo = new AllIndexFrMongoResultHandler2Page(indexObjOne, result,
                    paramMap, wtExeInsLstTmp, propsMap, allIndexTemplate, mongoTemplatePool,null)
            // 增量更新的情况
            if (operType == OPER_TYPE_SAVE) {
                resultHandler.mongoTemplatePool = mongoTemplatePool
                resultHandler.allIndexTemplate = allIndexTemplate
                resultHandler.saveIdxFormMongo(wtExeInsLstTmp, operType, propsMap, dsEngName, result)
            } else if (operType == OPER_TYPE_UPDATE) {
                String beginDateTime = DateUtils.date2String(getScanTime.get(BEG_TIME),DateUtils.FORMAT_DATETIME)
                String endDateTime = DateUtils.date2String(getScanTime.get(END_TIME),DateUtils.FORMAT_DATETIME)
                // 获取更新参数的sql
                String sql = StringUtils.ClobToString(indexObjOne.get("UPD_PARAM")).replace("\${BGN_TIME}", beginDateTime).
                        replace("\${END_TIME}", endDateTime)
//                rawSQLService.queryRawSql(sql, fullParamRowFromMongo)
                executeHandler(wtExeInsLst.get(0).get("db_name"),fullParamRowFromMongo,sql,null,indexObjOne.DS_ENG_NAME);
            }
        }
        updateIdxStatus(indexObjOne.DS_ENG_NAME,(Long)indexObjOne.DS_OBJ_ID,(Long)indexObjOne.DS_TYP, IDX_STAS_NORMAL)
        // 最后更新时间
        Date lastScanDate = new Date();
        if (getScanTime.get(END_TIME) != null) {
            lastScanDate = getScanTime.get(END_TIME)
        } else if (getScanTime.get(BEG_TIME) != null) {
            lastScanDate = getScanTime.get(BEG_TIME)
        }
        // 写入本次更新结束时间
        writeDsUpdTimeToMg(indexObjOne.DS_ENG_NAME,lastScanDate,MongoDBConfigConstants.IDX_STATUS_DB)
    }

    /**
     * 从关系型数据库更新指标数据
     * @param wtExeInsLstTmp
     * @param operType
     * @param scanTime
     */
    private void execAllIdxFormDB(List<Map<String,Object>> wtExeInsLst, String operType, Map<String,Date> scanTime) {
        List<Map<String,Object>> wtExeInsLstTmp = new ArrayList<Map<String,Object>>();
        wtExeInsLstTmp.addAll(wtExeInsLst)
        if (wtExeInsLstTmp.size() == 0) {
            return
        }
        Map<String,Object> indexMap = wtExeInsLstTmp.get(0)
        List<Map<String,Object>> props = xmlRawSQLService.queryRawSqlByKey("selectDopByObjId", [obj_id: indexMap.DS_OBJ_ID])
        updateIdxStatus(indexMap.DS_ENG_NAME, (Long)indexMap.DS_OBJ_ID,(Long)indexMap.DS_TYP, IDX_STAS_UPDATE)

        // 数据源是否带有参数
        if (indexMap.get("ds_typ") == 2 || (indexMap.get("ds_typ") == 6 && (indexMap.get("ds_param") == null
                || "" == indexMap.get("ds_param")))) {
            saveIdxFromDb(wtExeInsLstTmp, props, StringUtils.ClobToString(indexMap.get("SQL_CLAUSE")),null)
        } else if (indexMap.get("ds_typ") == 3) {
            // 带有参数的情况下  json格式解析成javaBean
            ObjectMapper mapper = new ObjectMapper()
            ParamObj[] result = mapper.readValue(indexMap.PARAM_DFT_VAL,ParamObj[].class)
            // 按照参数名称字母排序组成mongoDB集合名称
            Collections.sort(Arrays.asList(result))
            // 设置各参数类型
            Map paramMap = [:]
            // 数据源加入参数的SQL
            result.each{
                //获取各自参数的类型
                List<Map<String,Object>> paramTypes = xmlRawSQLService.queryRawSqlByKey("selectParamType", [param_code : it.type])
                if(paramTypes!=null&&paramTypes.size()>0){
                    paramMap.put(it.name, paramTypes.get(0).param_typ);
                }
            }

            //全量数据
            if (operType == OPER_TYPE_SAVE) {
                String sql = replaceSql(StringUtils.ClobToString(indexMap.SQL_CLAUSE), result);
//                List<Map<String, Object>> piecewiseList = rawSQLService.queryRawSql(indexMap.DS_PARAM_FULL);
                List<Map<String, Object>> piecewiseList = executeSqlByDbName(indexMap.db_name,indexMap.DS_PARAM_FULL, null,indexMap.DS_ENG_NAME)
                for (Map<String, Object> obj : piecewiseList) {
                    //设置参数值
                    Map<String, Object> sqlParams = new HashMap<String, Object>();
                    paramMap.each { key, value ->
                        if (value == 10) {
                            sqlParams.put(key, Long.valueOf(obj.get(key)+""))
                        } else if (value == 20) {
                            sqlParams.put(key, obj.get(key)+"")
                        } else if (value == 30) {
                            sqlParams.put(key, obj.get(key))
                        }
                    }
                    //保存数据
                    saveIdxFromDb(wtExeInsLstTmp, props, sql,sqlParams);
                }
            } else if (operType == OPER_TYPE_UPDATE) {
                String updParamsql = StringUtils.ClobToString(indexMap.DS_PARAM);
                updParamsql = replaceSql(updParamsql, [new ParamObj(name: "BGN_TIME"), new ParamObj(name: "END_TIME")] as ParamObj[])

                String sql = replaceSql(StringUtils.ClobToString(indexMap.SQL_CLAUSE), result);
                //查询分段数据
                Map<String, Object> sqlParam = new HashMap<String, Object>();
                sqlParam.put("BGN_TIME", DateUtils.date2String(scanTime.beginDateTime, DateUtils.FORMAT_DATETIME))
                sqlParam.put("END_TIME", DateUtils.date2String(scanTime.endDateTime, DateUtils.FORMAT_DATETIME))

//                List<Map<String, Object>> piecewiseList = rawSQLService.queryRawSql(updParamsql, sqlParam);
                List<Map<String, Object>> piecewiseList = executeSqlByDbName(indexMap.db_name,updParamsql, sqlParam,indexMap.DS_ENG_NAME)
                for (Map<String, Object> obj : piecewiseList) {
                    //设置参数值
                    Map<String, Object> sqlParams = new HashMap<String, Object>();
                    paramMap.each { key, value ->
                        if (value == 10) {
                            sqlParams.put(key, Long.valueOf(obj.get(key)+""))
                        } else if (value == 20) {
                            sqlParams.put(key, obj.get(key)+"")
                        } else if (value == 30) {
                            sqlParams.put(key, obj.get(key))
                        }
                    }
                    //保存数据
                    saveIdxFromDb(wtExeInsLstTmp, props, sql,sqlParams);
                }
            }
        } else if (indexMap.get("ds_typ") == 7) {
            ObjectMapper mapper = new ObjectMapper()
            ParamObj[] result = mapper.readValue(StringUtils.ClobToString(indexMap.UPD_KEY),ParamObj[].class)
            // 按照参数名称字母排序组成mongoDB集合名称
            Collections.sort(Arrays.asList(result))
            // 增量参数列表名称
            List params = []
            Map paramMap = [:]
            for (int i = 0; i < result.length; i++) {
                params.add(result[i].getName())
                // 获取各自参数的类型
                def paramTypes = xmlRawSQLService.queryRawSqlByKey("selectParamType", [param_code : result[i].getType()])
                paramMap.put(result[i].getName(), (Long)paramTypes[0].get("param_typ"))
            }
            if (operType == OPER_TYPE_SAVE) {
                String dsSql = StringUtils.ClobToString(indexMap.get("init_sql"))
                saveIdxFromDb(wtExeInsLstTmp, props, dsSql, null)
            } else if (operType == OPER_TYPE_UPDATE) {
                String sql = indexMap.SQL_CLAUSE;
                // 获取更新参数的sql
                String updParamsql = StringUtils.ClobToString(indexMap.get("UPD_PARAM"))
                updParamsql = replaceSql(updParamsql, [new ParamObj(name: "BGN_TIME"), new ParamObj(name: "END_TIME")] as ParamObj[])
                //查询分段数据
                Map<String, Object> sqlParam = new HashMap<String, Object>();
                sqlParam.put("BGN_TIME", DateUtils.date2String(scanTime.beginDateTime, DateUtils.FORMAT_DATETIME))
                sqlParam.put("END_TIME", DateUtils.date2String(scanTime.endDateTime, DateUtils.FORMAT_DATETIME))
                //获取更新时间段
//                List<Map<String, Object>> incrementalList = rawSQLService.queryRawSql(updParamsql, sqlParam);
                List<Map<String, Object>> incrementalList = executeSqlByDbName(indexMap.db_name,updParamsql, sqlParam,indexMap.DS_ENG_NAME)
                for (Map<String, Object> obj : incrementalList) {
                    //设置参数值
                    Map<String, Object> sqlParams = new HashMap<String, Object>();
                    paramMap.each { key, value ->
                        if (value == 10) {
                            sqlParams.put(key, Long.valueOf(obj.get(key)+""))
                        } else if (value == 20) {
                            sqlParams.put(key, obj.get(key)+"")
                        } else if (value == 30) {
                            sqlParams.put(key, obj.get(key))
                        }
                    }
                    //保存数据
                    saveIdxFromDb(wtExeInsLstTmp, props, sql,sqlParams);
                }
            }
        }
        // 结束操作更新状态
        updateIdxStatus(indexMap.DS_ENG_NAME, (Long)indexMap.DS_OBJ_ID,(Long)indexMap.DS_TYP, IDX_STAS_NORMAL)
        // 最后更新时间
        Date lastScanDate = new Date()
        if (scanTime.get(END_TIME) != null) {
            lastScanDate = scanTime.get(END_TIME)
        } else if (scanTime.get(BEG_TIME) != null) {
            lastScanDate = scanTime.get(BEG_TIME)
        }
        // 写入本次更新结束时间
        writeDsUpdTimeToMg(indexMap.DS_ENG_NAME, lastScanDate, MongoDBConfigConstants.IDX_STATUS_DB)
    }

    /***
     * 根据不同的db_name,执行handler
     */
    private void executeHandler(String db_name,ResultHandler resultHandler,String sql,Map params,String ds_name){
        String dbName = db_name == null? "default" : db_name;
        EbeanServer dbEbeanServer = null;
        try{
            dbEbeanServer = ebeanServerPool.getByName(dbName);
        }catch (e){
            dbEbeanServer = ebeanServerPool.getByName("default");
            logger.error("指标数据源【"+ds_name+"】获取数据连接失败，将使用默认连接尝试获取数据,错误信息："+e);
        }
        try{
            //判断是否为增量数据源的handler，如果是则需要将数据连接传递到handler里面
            if(resultHandler instanceof SubjectIncParamsResultHandler){
                resultHandler.dbEbeanServer = dbEbeanServer
            }
            logger.debug("executeHandler(): 保存数据源【"+  ds_name + "】的数据，获取数据源的数据连接。----"+dbName);
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
        }catch (err){
            logger.error("指标数据源【"+ds_name+"】执行sql错误：---"+err);
        }
    }

    /***
     * 根据不同的db_name,连接不同的数据库执行sql
     */
    private List<Map<String, Object>> executeSqlByDbName(String db_name,String sql,Map params,String ds_name){
        String dbName = db_name == null? "default" : db_name;
        EbeanServer dbEbeanServer = null;
        try{
            dbEbeanServer = ebeanServerPool.getByName(dbName);
        }catch (e){
            dbEbeanServer = ebeanServerPool.getByName("default");
            logger.error("指标数据源【"+ds_name+"】获取数据连接失败，将使用默认连接尝试获取数据,错误信息："+e);
        }
        try{
            logger.debug("executeSqlByDbName(): 连接【"+  ds_name + "】执行sql。----"+dbName);
            SqlQuery query = dbEbeanServer.createSqlQuery(sql);
            if(params) {
                params.each { key, value->
                    query.setParameter(key, value)
                }
            }
            return query.findList();
        }catch (err){
            logger.error("指标数据源【"+ds_name+"】执行sql错误：---"+err);
        }
        return null
    }
    /**
     * 修改最新更新时间
     * @param dsNm
     * @param updTime
     * @param dbNm
     */
    private  void writeDsUpdTimeToMg(String dsNm, Date updTime, String dbNm) {
		MongoOperations dsUpdTimeTemplate = mongoTemplatePool.getByName(dbNm)
		Map dsStatus = dsUpdTimeTemplate.findOne(new Query(Criteria.where("DS_NAME").is(dsNm)), Map.class,dbNm)
		if (null !=dsStatus ) {
			dsStatus.put("LAST_UPD_TIME", updTime)
            def up = EntityUtil.objectToUpdate(dsStatus)
            dsUpdTimeTemplate.updateFirst(new Query(Criteria.where("DS_NAME").is(dsNm)), up, dbNm)
		}
	}

    /**
     * 更新指标状态
     * @param dsName
     * @param dsObjId
     * @param dsTyp
     * @param status
     */
    private void updateIdxStatus(String dsName, Long dsObjId, Long dsTyp, int status) {
        MongoOperations idStatusTemplate = mongoTemplatePool.getByName(MongoDBConfigConstants.IDX_STATUS_DB)
        Map dsStatus = idStatusTemplate.findOne(new Query(Criteria.where("DS_NAME").is(dsName)), Map.class,MongoDBConfigConstants.IDX_STATUS_DB)
        if (null == dsStatus) {
            dsStatus = addIdxStatus(dsName, dsObjId, dsTyp)
            dsStatus.put("DS_STAS", status)
            idStatusTemplate.save(dsStatus, MongoDBConfigConstants.IDX_STATUS_DB)
        } else {
            dsStatus.put("DS_STAS", status)
            dsStatus.put("DS_UPD_TIME", new Date())
            def up = EntityUtil.objectToUpdate(dsStatus)
            idStatusTemplate.updateFirst(new Query(Criteria.where("DS_NAME").is(dsStatus.get(dsName))), up, MongoDBConfigConstants.IDX_STATUS_DB)
        }
    }

    /**
     * 添加指标状态数据
     * @param dsName
     * @param dsObjId
     * @param dsTyp
     * @return
     */
    private HashMap addIdxStatus(String dsName, Long dsObjId, Long dsTyp) {
        DsStatus dsStatus = new DsStatus()
        dsStatus.put("DS_NAME", dsName)
        dsStatus.put("DS_ID", dsObjId)
        dsStatus.put("DS_STAS", IDX_STAS_UPDATE)
        dsStatus.put("DS_TYPE", dsTyp)
        dsStatus.put("LAST_UPD_TIME", null)
        dsStatus.put("DS_UPD_TIME", new Date())
        dsStatus.put("DS_CRT_TIME", new Date())
        dsStatus.put("IDX_NAMES", null)
        return dsStatus
    }

    /**
     * 从关系型数据库保存指标数据
     * @param wtExeInsLst
     * @param props
     * @param sql
     * @param operType
     */
    private void saveIdxFromDb(List<Map<String,Object>> wtExeInsLst, List<Map<String,Object>> props,String sql,Map<String,Object> sqlParams) {
		// 带有参数的情况下
		ObjectMapper mapper = new ObjectMapper()
		ParamObj[] result =mapper.readValue(wtExeInsLst.get(0).get("idx_param"), ParamObj[].class)
		// 按照参数名称字母排序组成mongoDB集合名称
	    Collections.sort(Arrays.asList(result))
	    Map<String,Map<String,Object>> propsMap = getProsMap(props)
	    AllIndexFrDBResultHandler dsRow = new AllIndexFrDBResultHandler(result, propsMap, wtExeInsLst, allIndexTemplate)
		//判断是否有sql参数
        if(sqlParams!=null){
//            rawSQLService.queryRawSql(sql,sqlParams,dsRow)
            executeHandler(wtExeInsLst.get(0).get("db_name"),dsRow,sql,sqlParams,wtExeInsLst.get(0).get("DS_ENG_NAME"));
        }else{
//            rawSQLService.queryRawSql(sql, dsRow)
            executeHandler(wtExeInsLst.get(0).get("db_name"),dsRow,sql,null,wtExeInsLst.get(0).get("DS_ENG_NAME"));
        }
        //更新指标状态表中数据源中指标集合
        resultHandler.mongoTemplatePool = mongoTemplatePool;
        resultHandler.addIdxNmToStatus(wtExeInsLst)
	}

    /**
     * 获取扫描的时间段
     * @return
     */
    def getScanTime() {
        Map dateTimeMap = [:]
        def beginDateTime;
        def endDateTime ;
        String lastScanTimeStr = PropertiesUtil.getProperty(PropertiesUtil.LAST_SCAN_TIME_FILE,PropertiesUtil.LAST_SCAN_TIME)
        Date date = new Date()

        if (lastScanTimeStr == null || lastScanTimeStr == "") {
            endDateTime = DateUtils.date2String(date,DateUtils.FORMAT_DATETIME);
            beginDateTime = DateUtils.date2String(DateUtils.addMinutes(date, -10),DateUtils.FORMAT_DATETIME);
        } else {
            endDateTime = DateUtils.date2String(date, DateUtils.FORMAT_DATETIME);
            beginDateTime = lastScanTimeStr;
        }

        dateTimeMap.put(BEG_TIME, beginDateTime)
        dateTimeMap.put(END_TIME, endDateTime)

        return dateTimeMap
    }

    /**
     * 设置指标属性对象
     * @param props
     * @return
     */
    private Map<String,Map<String,Object>> getProsMap(List<Map<String,Object>> props) {
		Map<String,Map<String,Object>> map = [:]
        props.each{
            map.put(it.PROP_NAME, it)
        }
		return map
	}

    /**
     * 过滤数据源
     * @param dsname
     * @return
     */
    private List getNeedScanList(List<Map<String,Object>> dsLst){
		List<Map<String,Object>> result = []
		for (ds in dsLst) {
            if (isNeedScan(ds.DS_ENG_NAME)) {
                result.add(ds)
            }
        }
		return result
	}

    /**
     * 判断数据源是否需要更新
     * @param dsname
     * @return
     */
    private boolean isNeedScan(String dsname){
		if(m_ExcludeDS != null && m_ExcludeDS.size() > 0){
			return !m_ExcludeDS.contains(dsname.toUpperCase())
		}
		if(m_IncludeDS != null && m_IncludeDS.size() > 0){
			return m_IncludeDS.contains(dsname.toUpperCase())
		}
		return true
	}

    /**
     * 参数替换
     * @param sql
     * @return
     */
    String replaceSql(String sql, ParamObj[] params) {
        params.each {
            String key = '\${' + it.name + '}'
            while (sql.indexOf(key) != -1) {
                if (sql.indexOf("\'" + key + "\'") > 0) {
                    key = "\'" + key + "\'"
                }
                sql = sql.replace(key, ':' + it.name);
            }
        }
        return sql
    }
	
}
