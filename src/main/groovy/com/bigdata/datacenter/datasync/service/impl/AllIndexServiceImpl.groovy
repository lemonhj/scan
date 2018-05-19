package com.bigdata.datacenter.datasync.service.impl

import com.bigdata.datacenter.datasync.core.data.MongoTemplatePool
import com.bigdata.datacenter.datasync.service.ScanService
import com.bigdata.datacenter.datasync.service.SubjectStatusService
import com.bigdata.datacenter.datasync.service.XmlRawSQLService
import com.bigdata.datacenter.datasync.utils.DateUtils
import com.bigdata.datacenter.datasync.utils.PropertiesUtil
import com.bigdata.datacenter.datasync.utils.constants.MongoDBConfigConstants
import com.bigdata.datacenter.datasync.model.mongodb.DsStatus
import com.bigdata.datacenter.datasync.model.mongodb.ParamObj
import com.bigdata.datacenter.datasync.model.mongodb.SubjectStatus
import com.bigdata.datacenter.metadata.service.RawSQLService
import com.bigdata.datacenter.metadata.utils.EntityUtil
import com.bigdata.datacenter.metadata.utils.StringUtils
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.data.mongodb.core.MongoOperations
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.Update
import org.springframework.stereotype.Service

import java.sql.SQLException
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import org.springframework.data.mongodb.core.query.Criteria

import java.util.regex.Pattern


/**
 * Created by Dan on 2017/6/20.
 */
@Service(value = "all-index")
class AllIndexServiceImpl implements ScanService {

    private static final Logger logger = Logger.getLogger(AllIndexServiceImpl.class)
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
    @Qualifier("subjectStatusServiceImpl")
    SubjectStatusService subjectStatusService
    @Autowired
    MongoTemplatePool mongoTemplatePool
    MongoOperations allIndexTemplate
    AllIndexFrDsResultHandler frDsResultHandler = new AllIndexFrDsResultHandler()

    //全量更新
    @Override
    void totalSync() {

        def operType = OPER_TYPE_SAVE
        allIndexTemplate = mongoTemplatePool.getByName(MongoDBConfigConstants.INDEX_ALL_DB)
        List indexLst = xmlRawSQLService.queryRawSqlByKey("selectIndexAll")
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
        Map dsTimeMap = [:]
        Map timeMap = getScanTime()
        // 过滤指标结果集
        execFirInsLst(indexLst, operType, dsTimeMap, timeMap)
    }

    //增量跟新
    @Override
    void incrementalSync() {
        def operType = OPER_TYPE_UPDATE
        def idxFrSub = PropertiesUtil.getProperty("idx.fr.sub")
        Map timeMap = getScanTime()
        allIndexTemplate = mongoTemplatePool.getByName(MongoDBConfigConstants.INDEX_ALL_DB)

        List dsNmLst = []
        Map dsTimeMap = [:]
        Map tblTimeMap = [:]
        List dataSrcs = []

        if (idxFrSub.equals("true")) {
            //从专题的mongoDB更新
            dataSrcs = getChgTbl(MongoDBConfigConstants.IDX_TBL_UPD_TIME, MongoDBConfigConstants.IDX_STATUS_DB,
                    timeMap, dsTimeMap, tblTimeMap)
        }else {
            //从数据库更新
            //SubjectServiceImpl subService = new SubjectServiceImpl()
            dataSrcs = getChgTbls(MongoDBConfigConstants.IDX_TBL_UPD_TIME, timeMap, tblTimeMap, dsTimeMap)
        }
        dataSrcs = getNeedScanList(dataSrcs)

        for (int i = 0; i < dataSrcs.size(); i++) {
            dsNmLst.add(dataSrcs.get(i).get("ds_eng_name"))
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
        execFirInsLst(indexLst, operType, dsTimeMap, timeMap)
    }

    public void UpdateIdxProChgToMgDB() {
        String operType = OPER_TYPE_SAVE
        if (PropertiesUtil.getProperty("index.prochg").equals("false")){
            return
        }
        allIndexTemplate = mongoTemplatePool.getByName(MongoDBConfigConstants.INDEX_ALL_DB)
        String lastIdxChgDate = PropertiesUtil.getProperty(PropertiesUtil.LAST_SCAN_TIME_FILE,PropertiesUtil.LAST_IDX_CHG_DATE)
        String lastIdxChgEndDate = DateUtils.date2String(new Date(), DateUtils.FORMAT_DATE)

        Map lastScanChgProTime = [:]
        lastScanChgProTime.put("beginDateTime", lastIdxChgDate)
        lastScanChgProTime.put("endDateTime", lastIdxChgEndDate)
        //指标列表
        List indexLst = rawSQLService.queryRawSqlByKey("selectIdxByUdpTime", lastScanChgProTime)
        Map dsTimeMap = [:]
        Map timeMap = [:]
        // 过滤指标结果集
        execFirInsLst(indexLst, operType, dsTimeMap, timeMap)
        // 写入下次更新参数时间
        //全扫描则创建下次更新扫描开始时间
        PropertiesUtil.writeProperties(PropertiesUtil.LAST_SCAN_TIME_FILE,PropertiesUtil.LAST_IDX_CHG_DATE, lastIdxChgEndDate)
    }

    public void updateMarcoIndexToMgDB() {
        MongoOperations idMarcoTemplate = mongoTemplatePool.getByName(MongoDBConfigConstants.IDX_MARCO)
        List dataSrcs = rawSQLService.queryRawSqlByKey("selectMarcoDataSrc")
        for (int i = 0; i < dataSrcs.size(); i++) {
            Map dataSrc = dataSrcs.get(i)
            logger.info("updateMarcoIndexToMgDB:" + dataSrc.get("ds_eng_name") + ":start")
            long start = System.currentTimeMillis()
            List props = rawSQLService.queryRawSqlByKey("selectDopByObjId", [obj_id : dataSrc.get("obj_id")])
            Date oldUpdTime = getMacroMaxUpdTime(idMarcoTemplate, dataSrc.get("ds_eng_name"))

            AllIndexMacroDsResultHandler dsRow = new AllIndexMacroDsResultHandler(idMarcoTemplate,
                    props, oldUpdTime, dataSrc.get("ds_eng_name"))

            rawSQLService.queryRawSql(StringUtils.ClobToString(dataSrc.get("sql_clause")) + " where UPD_TIME >= TO_DATE('" +
                    DateUtils.date2String(oldUpdTime, DateUtils.FORMAT_DATETIME) + "', " +
                    "'YYYY-MM-DD HH24:MI:SS')" + " order by UPD_TIME asc", dsRow)
        }
    }

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

    List<Map<String,Object>> getChgTbls(String colNm, Map dateTimeMap, Map tblTimeMap, Map dsTimeMap){
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

    public List getChgTbl(String colName,String idxStatusDb,Map dateTimeMap,Map dsTimeMap,Map tblTimeMap ) {
        def TBL_NAME = "TBL_NAME"
        def UPD_TIME = "UPD_TIME"
        List dsLst = []
        List needTbls = []

        MongoOperations idxUpdTimeTemplate = mongoTemplatePool.getByName(colName)
        MongoOperations subUpdTimeTemplate = mongoTemplatePool.getByName(MongoDBConfigConstants.SUB_TBL_UPD_TIME)
        List curs = subUpdTimeTemplate.findAll(DsStatus.class, MongoDBConfigConstants.SUB_TBL_UPD_TIME)

        for (var in curs) {
            def tblNm = var.get("TBL_NAME")
            def maxDate = var.get("UPD_TIME")
            def beginDate = DateUtils.string2Date(dateTimeMap.get(BEG_TIME), DateUtils.FORMAT_DATETIME)
            Map lastObj = idxUpdTimeTemplate.findOne(new Query(Criteria.where("TBL_NAME").is(tblNm)), Map.class, colName)
            Date lastUpdTime = null
            boolean noFlg = false

            if (null == lastObj) {
                noFlg = true
                lastUpdTime = beginDate
                lastObj.clear()
                lastObj.put(TBL_NAME, tblNm)
                // 保存表最新更新时间
                lastObj.put(UPD_TIME, maxDate)
                tblTimeMap.put(tblNm, lastObj)
            } else {
                lastUpdTime = (Date) lastObj.get(UPD_TIME)
                lastObj.put(UPD_TIME, maxDate)
                tblTimeMap.put(tblNm, lastObj)
            }
            // 判断最新数据时间是否在扫描时间周期内
            if (noFlg || maxDate.compareTo(lastUpdTime) > 0) {
                List tbl = []
                tbl.add(tblNm)
                List dataSrcs = xmlRawSQLService.queryRawSqlByKey("selectDataSrcByTbl", [item: tbl])
                // 获取每个源上次更新时间及设置本次更新时间
                for (int j = 0; j < dataSrcs.size(); j++) {
                    String dsNm = dataSrcs.get(j).get("DsEngName")
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
                    if (!dsNms.contains(dataSrcs.get(j).get("ds_eng_name"))) {
                        dsNms.add(dataSrcs.get(j).get("ds_eng_name"))
                        dsLst.add(dataSrcs.get(j))
                    }
                }
                continue
            }

        }
		return dsLst
    }

    public Date readDsUpdTimeFrMg(String dsNm,String dbNm) {
		Date updTime = null
		MongoOperations dsColTemplate = mongoTemplatePool.getByName(dbNm)
		DsStatus dsStatus = dsColTemplate.findOne(new Query(Criteria.where("ds_name").is(dsNm)), Map.class, dbNm)
		if (null !=dsStatus ) {
			updTime = (Date)dsStatus.get("last_upd_time")
		}
		return updTime
	}

    void execFirInsLst(List indexLst, String operType, Map dsTimeMap, Map timeMap) {
        //处理过的指标id存储的位置
        List indexNmLst = []
        List wtExeInsLst = []
        def dsEngName = ""
        //创建线程池
        ThreadPoolExecutor producerPool = new ThreadPoolExecutor(1, 1, 1,
                TimeUnit.SECONDS, new LinkedBlockingQueue(),
                new ThreadPoolExecutor.CallerRunsPolicy())
        //循环遍历指标进行数据处理
        for (indexObj in indexLst) {
            if (indexNmLst.contains(indexObj.get("idx_id"))) {
                continue
            }
            indexNmLst.add(indexObj.get("idx_id"))
            // 记录上次的数据源名称,不同则一组指标更新
            if (!"".equals(dsEngName) && !dsEngName.equals(indexObj.get("ds_eng_name"))) {
                Map dsMap = dsTimeMap.get(dsEngName)
                if (dsMap == null) {
                    dsMap = [:]
                    dsMap.put(BEG_TIME, DateUtils.string2Date(timeMap.get(BEG_TIME), DateUtils.FORMAT_DATETIME))
                    dsMap.put(END_TIME, DateUtils.string2Date(timeMap.get(END_TIME), DateUtils.FORMAT_DATETIME))
                    dsTimeMap.put(dsEngName, dsMap)
                }
                producerPool.execute(new ThreadPoolTaskM(wtExeInsLst, operType,
                        dsTimeMap.get(dsEngName), dsEngName, PropertiesUtil.getProperty("idx.fr.sub")));
//				execInsLst(wtExeInsLst,db,operType,dsMap);
                wtExeInsLst.clear()
            }
            wtExeInsLst.add(indexObj)
            dsEngName = indexObj.get("ds_eng_name")
        }
        //循环外再做一次
        if (wtExeInsLst.size() != 0) {
            Map dsMap = dsTimeMap.get(dsEngName)

            if (dsMap == null) {
                dsMap = [:]
                dsMap.put(BEG_TIME, DateUtils.string2Date(timeMap.get(BEG_TIME), DateUtils.FORMAT_DATETIME))
                dsMap.put(END_TIME, DateUtils.string2Date(timeMap.get(END_TIME), DateUtils.FORMAT_DATETIME))
                dsTimeMap.put(dsEngName, dsMap)
            }
            producerPool.execute(new ThreadPoolTaskM(wtExeInsLst, operType,
                    dsTimeMap.get(dsEngName), dsEngName, PropertiesUtil.getProperty("idx.fr.sub")))
        }

        if (producerPool != null) {
            producerPool.shutdown()
            while (!producerPool.awaitTermination(1, TimeUnit.SECONDS)) {
            }
            // 关闭线程池
            logger.info(operType + "IndexToMgDB" + " CompletedTaskCount:" + producerPool.getCompletedTaskCount());
        }

    }

    /*多线程保存数据至mongoDB*/

    private class ThreadPoolTaskM implements Runnable, Serializable {

        private List wtExeInsLst
        private String operType
        private Map timeMap
        private String dsEngName
        private String idxFrSubFlg

        public ThreadPoolTaskM(List wtExeInsLst, String operType, Map timeMap,
                               String dsEngName, String idxFrSubFlg) {
            this.wtExeInsLst = wtExeInsLst
            this.operType = operType
            this.timeMap = timeMap
            this.dsEngName = dsEngName
            this.idxFrSubFlg = idxFrSubFlg
        }

        @Override
        public void run() {
            try {
                // 是否从mongoDB更新
                if (idxFrSubFlg.equals("true")) {
                    execInsLst_frDs(wtExeInsLst, operType, timeMap);
                } else {
                    execInsLst(wtExeInsLst, operType, timeMap);
                }
            } catch (Exception e) {
                logger.error(e.toString());
            }
        }
    }

    private void execInsLst_frDs(List wtExeInsLst, String operType, Map getScanTime) {
        if (wtExeInsLst.size() == 0) {
			return
		}
        Map indexObjOne = wtExeInsLst.get(0)
        String dsEngName = indexObjOne.get("ds_eng_name")

        List props = xmlRawSQLService.queryRawSqlByKey("selectDopByObjId", [obj_id: wtExeInsLst[0].get("ds_obj_id").longValue()])
        updateIdxStatus(dsEngName,indexObjOne.get("ds_obj_id").longValue(), indexObjOne.get("ds_typ").longValue(),IDX_STAS_UPDATE)

        ObjectMapper mapper = new ObjectMapper()
        ParamObj[] result =mapper.readValue(indexObjOne.get("idx_param"), ParamObj[].class)
        Map propsMap = getProsMap(props)

        //构造一个线程池
        ThreadPoolExecutor producerPool = null
        String objId = String.valueOf(indexObjOne.get("obj_id").intValue())

        if (indexObjOne.get("DS_TYP") == 2 || (indexObjOne.get("DS_TYP") == 6 && (indexObjOne.get("DS_PARAM") == null
                || indexObjOne.get("DS_PARAM").equals("")))) {
            frDsResultHandler.execIndexObj_frD(wtExeInsLst, operType, propsMap, dsEngName, dsEngName, result, objId)
        } else if (indexObjOne.get("DS_TYP") == 3)  {
            ObjectMapper dsMapper = new ObjectMapper();
            ParamObj[] dsResult =dsMapper.readValue(indexObjOne.get("param_dft_val"), ParamObj[].class)
            // 按照参数名称字母排序组成mongoDB集合名称
            Collections.sort(Arrays.asList(result))
            // 设置各参数类型
            Map paramMap = [:]
            for (int j = 0; j < result.length; j++) {
                //获取各自参数的类型
                def paramTypes = xmlRawSQLService.queryRawSqlByKey("selectParamType", [param_code : result[j].getType()])
                paramMap.put(result[j].getName(), (Long)paramTypes[0].get("param_typ"))
            }
            producerPool = new ThreadPoolExecutor(20, 20, 1, TimeUnit.SECONDS, new LinkedBlockingQueue(),
            new ThreadPoolExecutor.CallerRunsPolicy())
            AllIndexFrDsResultHandler fullParamRow_frDs = new AllIndexFrDsResultHandler(indexObjOne, result,
                    paramMap, wtExeInsLst, operType, producerPool, propsMap, dsResult, allIndexTemplate, mongoTemplatePool)
            if (operType.equals(OPER_TYPE_SAVE)) {
                String sql = indexObjOne.get("ds_param_full")
                rawSQLService.queryRawSql(sql, fullParamRow_frDs)
            }else if (operType.equals(OPER_TYPE_UPDATE)) {

                String beginDateTime = DateUtils.date2String(getScanTime.get(BEG_TIME),DateUtils.FORMAT_DATETIME);
                String endDateTime = DateUtils.date2String(getScanTime.get(END_TIME),DateUtils.FORMAT_DATETIME);
                // 获取更新参数的sql
                String sql = StringUtils.ClobToString(indexObjOne.get("DS_PARAM")).replace("\${BGN_TIME}", beginDateTime).
                        replace("\${END_TIME}", endDateTime)
                rawSQLService.queryRawSql(sql, fullParamRow_frDs)
            }
        } else if (indexObjOne.get("DS_TYP") == 7) {
            // 解析默认参数
            ObjectMapper dsMapper = new ObjectMapper()
            ParamObj[] incDsResult =dsMapper.readValue(indexObjOne.get("UPD_KEY"), ParamObj[].class)
            // 按照参数名称字母排序组成mongoDB集合名称
            Collections.sort(Arrays.asList(result))
            // 设置各参数类型
            Map paramMap = [:]
            // 数据源加入参数的SQL
            for (int j = 0; j < result.length; j++) {
                // 获取各自参数的类型
                def paramTypes = xmlRawSQLService.queryRawSqlByKey("selectParamType", [obj_id : result[j].getType()])
                paramMap.put(result[j].getName(), (Long)paramTypes[0].get("param_typ"))
            }
            producerPool = new ThreadPoolExecutor(20, 20, 1, TimeUnit.SECONDS, new LinkedBlockingQueue(),
                    new ThreadPoolExecutor.CallerRunsPolicy())
            AllIndexFrDsResultHandler fullParamRow_frDs = new AllIndexFrDsResultHandler(indexObjOne, result,
                    paramMap, wtExeInsLst, operType, producerPool, propsMap, incDsResult, allIndexTemplate, mongoTemplatePool)
            // 增量更新的情况
            if (operType.equals(OPER_TYPE_SAVE)) {
                frDsResultHandler.execIndexObj_frD(wtExeInsLst, operType, propsMap, dsEngName, dsEngName, result, objId)
            } else if (operType.equals(OPER_TYPE_UPDATE)) {
                String beginDateTime = DateUtils.date2String(getScanTime.get(BEG_TIME),DateUtils.FORMAT_DATETIME)
                String endDateTime = DateUtils.date2String(getScanTime.get(END_TIME),DateUtils.FORMAT_DATETIME)
                // 获取更新参数的sql
                String sql = StringUtils.ClobToString(indexObjOne.get("UPD_PARAM")).replace("\${BGN_TIME}", beginDateTime).
                        replace("\${END_TIME}", endDateTime)
                rawSQLService.queryRawSql(sql, fullParamRow_frDs)
            }
        }
        if (producerPool != null) {
            // 关闭线程池
            producerPool.shutdown()
            while (!producerPool.awaitTermination(1, TimeUnit.SECONDS)) {
            }
        }
        updateIdxStatus(indexObjOne.get("DS_ENG_NAME"),indexObjOne.get("DS_OBJ_ID").longValue(),
                indexObjOne.get("DS_TYP").longValue(), IDX_STAS_NORMAL)
        // 最后更新时间
        Date lastScanDate = new Date();
        if (getScanTime.get(END_TIME) != null) {
            lastScanDate = getScanTime.get(END_TIME)
        } else if (getScanTime.get(BEG_TIME) != null) {
            lastScanDate = getScanTime.get(BEG_TIME)
        }
        // 写入本次更新结束时间
        writeDsUpdTimeToMg(indexObjOne.get("DS_ENG_NAME"),lastScanDate,MongoDBConfigConstants.IDX_STATUS_DB)
    }

    /*private void execIndexObj_frD(List wtExeInsLst, String operType, Map propsMap,
              String dsEngName, String colName, ParamObj[] result, String objId) {

        List cur = []
        MongoOperations dsColTemplate = mongoTemplatePool.getByName(colName)

        cur = dsColTemplate.findAll(DsStatus.class, MongoDBConfigConstants.SUB_TBL_UPD_TIME)
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
                obj.put(KEY, kyeTmp)
                obj.put(Value, var.get(indexObj.get("FLD_NAME")))
                // 保存数据到mongoDB
                if (OPER_TYPE_SAVE.equals(operType)) {
                    allIndexTemplate.save(obj, colNm)
                } else if (OPER_TYPE_UPDATE.equals(operType)) {
                    def up = EntityUtil.objectToUpdate(obj)
                    allIndexTemplate.updateFirst(new Query(Criteria.where(KEY).is(kyeTmp)), up, colNm)
                }
            }

			for (int j = 0; j < wtExeInsLst.size(); j++) {
				Map indexObj = wtExeInsLst.get(j)
				addIdxNmToStatus(indexObj.get("DS_ENG_NAME"),indexObj.get("IDX_NAME"),indexObj.get("FLD_NAME"));
			}
        }

    }*/

    private void execInsLst(List wtExeInsLstTmp, String operType, Map scanTime) {
        Map indexMap = wtExeInsLstTmp.get(0)
        def objId = indexMap.get("ds_obj_id").longValue()
        List props = xmlRawSQLService.queryRawSqlByKey("selectDopByObjId", [obj_id: indexMap.get("ds_obj_id").longValue()])
        updateIdxStatus(indexMap.get("ds_eng_name"), indexMap.get("ds_obj_id").longValue(),
                indexMap.get("ds_typ").longValue(), IDX_STAS_UPDATE)
        // 解析默认参数
        def defaultParam = indexMap.get("param_dft_val")
        //构造一个线程池
        ThreadPoolExecutor producerPool = null
        // 数据源是否带有参数
        if (indexMap.get("ds_typ") == 2 || (indexMap.get("ds_typ") == 6 && (indexMap.get("ds_param") == null
                || indexMap.get("ds_param").equals("")))) {
            execIndexObj(wtExeInsLstTmp, props, StringUtils.ClobToString(indexMap.get("SQL_CLAUSE")), operType)
        } else if (indexMap.get("ds_typ") == 3) {
            // 带有参数的情况下  json格式解析成javaBean
            ObjectMapper mapper = new ObjectMapper()
            ParamObj[] result = mapper.readValue(defaultParam,
                    ParamObj[].class)
            // 按照参数名称字母排序组成mongoDB集合名称
            Collections.sort(Arrays.asList(result))
            // 设置各参数类型
            Map paramMap = [:]
            // 数据源加入参数的SQL
            for (int j = 0; j < result.length; j++) {
                // 获取各自参数的类型
                def paramTypes = xmlRawSQLService.queryRawSqlByKey("selectParamType",[param_code:result[j].getType()])
                paramMap.put(result[j].getName(), (Long)paramTypes[0].get("param_typ"))
            }
            producerPool = new ThreadPoolExecutor(20, 20, 1,
						TimeUnit.SECONDS, new LinkedBlockingQueue(),
						new ThreadPoolExecutor.CallerRunsPolicy())
            AllIndexParamHandler fullParamRow = new AllIndexParamHandler(indexMap, result,
                    paramMap, props, wtExeInsLstTmp, allIndexTemplate, operType, producerPool,
                    StringUtils.ClobToString(indexMap.get("sql_clause")), rawSQLService, mongoTemplatePool)

            if (operType.equals(OPER_TYPE_SAVE)) {
                String dsSql = indexMap.get("ds_param_full")
                //回调并保存到mongodb
                rawSQLService.queryRawSql( dsSql, fullParamRow)
            } else if (operType.equals(OPER_TYPE_UPDATE)) {
                def beginDateTime = DateUtils.date2String(scanTime.get(BEG_TIME),DateUtils.FORMAT_DATETIME)
                def endDateTime = DateUtils.date2String(scanTime.get(END_TIME),DateUtils.FORMAT_DATETIME)
                String sql = StringUtils.ClobToString(indexMap.get("ds_param"))
                sql = sql.replace("\${BGN_TIME}", beginDateTime).replace("\${END_TIME}", endDateTime)
                rawSQLService.queryRawSql(sql, fullParamRow)
            }
        } else if (indexMap.get("ds_typ") == 7) {
            ObjectMapper mapper = new ObjectMapper()
            ParamObj[] result = mapper.readValue(StringUtils.ClobToString(indexMap.get("upd_key")),
                    ParamObj[].class)
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
            producerPool = new ThreadPoolExecutor(20, 20, 1,
                    TimeUnit.SECONDS, new LinkedBlockingQueue(),
                    new ThreadPoolExecutor.CallerRunsPolicy())
            if (operType.equals(OPER_TYPE_SAVE)) {
                String dsSql = StringUtils.ClobToString(indexMap.get("init_sql"))
                execIndexObj(wtExeInsLstTmp, props, dsSql, operType)
            } else if (operType.equals(OPER_TYPE_UPDATE)) {
                String beginDateTime = DateUtils.date2String(scanTime.get(BEG_TIME),DateUtils.FORMAT_DATETIME);
                String endDateTime = DateUtils.date2String(scanTime.get(END_TIME),DateUtils.FORMAT_DATETIME);
                // 获取更新参数的sql
                String dsSql = StringUtils.ClobToString(indexMap.get("UPD_PARAM"))
                dsSql = dsSql.replace("\${BGN_TIME}", beginDateTime).replace("\${END_TIME}", endDateTime)

                AllIndexParamHandler fullParamRow = new AllIndexParamHandler(indexMap, result, paramMap, props,
                        wtExeInsLstTmp, operType, producerPool, indexMap.get("SQL_CLAUSE"), rawSQLService, mongoTemplatePool)
                rawSQLService.queryRawSql(dsSql, fullParamRow)
            }
        }
        if (producerPool != null) {
            // 关闭线程池
            producerPool.shutdown()
            while (!producerPool.awaitTermination(1, TimeUnit.SECONDS)) {
            }
            logger.info(indexMap.DS_ENG_NAME + " " + "CompletedTaskCount:" + producerPool.getCompletedTaskCount())
        }
        // 结束操作更新状态
        updateIdxStatus(indexMap.get("ds_eng_name"), indexMap.get("ds_obj_id").longValue(),
                indexMap.get("ds_typ").longValue(), IDX_STAS_NORMAL)
        // 最后更新时间
        Date lastScanDate = new Date()
        if (scanTime.get(END_TIME) != null) {
            lastScanDate = scanTime.get(END_TIME)
        } else if (scanTime.get(BEG_TIME) != null) {
            lastScanDate = scanTime.get(BEG_TIME)
        }
        // 写入本次更新结束时间
        writeDsUpdTimeToMg(indexMap.get("ds_eng_name"), lastScanDate, MongoDBConfigConstants.IDX_STATUS_DB)
    }

    private  void writeDsUpdTimeToMg(String dsNm, Date updTime, String dbNm) {
		MongoOperations dsUpdTimeTemplate = mongoTemplatePool.getByName(dbNm)
		Map dsStatus = dsUpdTimeTemplate.findOne(new Query(Criteria.where("DS_NAME").is(dsNm)), Map.class)
		if (null !=dsStatus ) {
			dsStatus.put("LAST_UPD_TIME", updTime)
            def up = EntityUtil.objectToUpdate(dsStatus)
            dsUpdTimeTemplate.updateFirst(new Query(Criteria.where("DS_NAME").is(dsNm)), up, dbNm)
		}
	}

    private void updateIdxStatus(String dsName, Long dsObjId, Long dsTyp, int status) {
        MongoOperations idStatusTemplate = mongoTemplatePool.getByName(MongoDBConfigConstants.IDX_STATUS_DB)
        Map dsStatus = idStatusTemplate.findOne(new Query(Criteria.where("DS_NAME").is(dsName)), Map.class)
        if (null == dsStatus) {
            dsStatus = addIdxStatus(dsName, dsObjId, dsTyp)
            dsStatus.put("DS_STAS", status)
            idStatusTemplate.save(dsStatus, dsName)
        } else {
            dsStatus.put("DS_STAS", status)
            dsStatus.put("DS_UPD_TIME", new Date())
            def up = EntityUtil.objectToUpdate(dsStatus)
            idStatusTemplate.updateFirst(new Query(Criteria.where("DS_NAME").is(dsStatus.get(dsName))), up, dsName)
        }
    }

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
			idxNms.add(obj);
			dsStatus.put("IDX_NAMES", idxNms)
		}
		def up = EntityUtil.objectToUpdate(dsStatus)
        dsStatusTemplate.updateFirst(new Query(Criteria.where("DS_NAME").is(dsStatus.get(dsName))), up, dsName)
	}

    /**
	 * 遍历所有IDX_ID，删除不存在的指标数据或者已修改参数后残留数据。
	 */
    public void delDirtyDataToMgDB() {
        int lastScanIdx = 1
        int initalDelIdxCnt = 10
        Map idxMap = [:]
        List indexIdxLst = []
        String db_type = PropertiesUtil.getProperty("db.type")
        allIndexTemplate = mongoTemplatePool.getByName(MongoDBConfigConstants.INDEX_ALL_DB)
        List indexLst = rawSQLService.queryRawSql("selectIndexAll")
        for (int i = 0; i < indexLst.size(); i++) {
            int _idx = indexLst.get(i).get("IDX_ID").intValue()
            if (!indexIdxLst.contains(_idx)) {
                indexIdxLst.add(_idx)
                idxMap.put(_idx, indexLst.get(i))
            }
        }
        Collections.sort(indexIdxLst)
        int maxScanIdx = indexIdxLst.get(indexIdxLst.size() - 1)
        String lastScnIdxStr = readDsUpdTime()
        if (lastScnIdxStr != null) {
            lastScanIdx = Integer.valueOf(lastScnIdxStr) + 1
        }
        int cnt = 1
        int delCount = 0
        while (true) {
            if (lastScanIdx > maxScanIdx) {
                lastScanIdx = 1
            }
            Map key = [:]
            Pattern pattern = Pattern.compile("^" + lastScanIdx + "_.*\$")
            key.put("Key", pattern)
            List cur = allIndexTemplate.find(new Query(Criteria.where("Key").is(pattern), Map.class,
                    MongoDBConfigConstants.INDEX_ALL_DB))
            logger.info("scan idx  id-" + lastScanIdx + ": start")
            if (!indexIdxLst.contains(lastScanIdx)) {
                for (var in cur) {
                    allIndexTemplate.remove(var, MongoDBConfigConstants.INDEX_ALL_DB)
                    delCount++
                }
                logger.info("delete dirty index " + pattern + ":" + delCount);
            } else {
                String idxParam = idxMap.get(lastScanIdx).get("IDX_PARAM")
                // 带有参数的情况下
                ObjectMapper mapper = new ObjectMapper();
                // 指标参数
                ParamObj[] result = mapper.readValue(idxParam,
                        ParamObj[].class)
                for (var in cur) {
                    String keyStr = var.get("Key")
                    // IDX_参数 = 参数 + 1
                    if (keyStr.split("_").length != result.length + 1) {
                        allIndexTemplate.remove(var, MongoDBConfigConstants.INDEX_ALL_DB)
                        delCount++;
                    }
                }
                logger.info("delete dirty index " + pattern + ":" + delCount);
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
    }

    def readDsUpdTime(){
        String lastScanTimeStr = PropertiesUtil.getProperty(PropertiesUtil.LAST_SCAN_TIME_FILE,PropertiesUtil.LAST_SCAN_IDX)
        return lastScanTimeStr
    }

    def getScanTime() {
        Map dateTimeMap = [:]
        def beginDateTime = null
        def endDateTime = null
        String lastScanTimeStr = PropertiesUtil.getProperty(PropertiesUtil.LAST_SCAN_TIME_FILE,PropertiesUtil.LAST_SCAN_TIME)
        Date date = new Date()

        if (lastScanTimeStr == null || lastScanTimeStr.equals("")) {
            endDateTime = DateUtils.date2String(date,
                    DateUtils.FORMAT_DATETIME);
            beginDateTime = DateUtils
                    .date2String(DateUtils.addMinutes(date, -10),
                    DateUtils.FORMAT_DATETIME);
        } else {
            endDateTime = DateUtils.date2String(date,
                    DateUtils.FORMAT_DATETIME);
            beginDateTime = lastScanTimeStr;
        }

        dateTimeMap.put(BEG_TIME, beginDateTime)
        dateTimeMap.put(END_TIME, endDateTime)

        return dateTimeMap
    }




    private Map getProsMap(List props) {
		Map map = [:]
		for (int k = 0; k < props.size(); k++) {
			map.put(props.get(k).get("prop_name"), props.get(k))
		}
		return map
	}

    private String getProType(String fldName, Map prosMap) {
		String rtn = ""
		Map prop = prosMap.get(fldName)
		if (null != prop) {
			rtn = prop.get("prop_typ")
		}
		return rtn
	}

    private List getNeedScanList(List dsLst){
		List result = []
		for (ds in dsLst) {
            if (isNeedScan(ds.get("ds_eng_name"))) {
                result.add(ds)
            }
        }
		return result
	}

    private boolean isNeedScan(String dsname){
		if(m_ExcludeDS != null && m_ExcludeDS.size() > 0){
			return !m_ExcludeDS.contains(dsname.toUpperCase())
		}
		if(m_IncludeDS != null && m_IncludeDS.size() > 0){
			return m_IncludeDS.contains(dsname.toUpperCase())
		}
		return true
	}
}
