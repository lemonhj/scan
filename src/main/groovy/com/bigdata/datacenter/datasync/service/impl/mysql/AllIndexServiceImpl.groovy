package com.bigdata.datacenter.datasync.service.impl.mysql

import com.bigdata.datacenter.datasync.model.mongodb.DsStatus
import com.bigdata.datacenter.datasync.model.mongodb.ParamObj
import com.bigdata.datacenter.datasync.service.ScanService
import com.bigdata.datacenter.datasync.service.XmlRawSQLService
import com.bigdata.datacenter.datasync.service.impl.MailService
import com.bigdata.datacenter.datasync.utils.DateUtils
import com.bigdata.datacenter.datasync.utils.PropertiesUtil
import com.bigdata.datacenter.datasync.utils.constants.MongoDBConfigConstants
import com.bigdata.datacenter.metadata.service.RawSQLService
import com.bigdata.datacenter.metadata.service.ResultHandler;
import com.bigdata.datacenter.metadata.utils.StringUtils
import com.fasterxml.jackson.databind.ObjectMapper

import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

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
//@Service(value = "all-index-mysql")
class AllIndexServiceImpl implements ScanService {

    private static final Logger logger = Logger.getLogger(AllIndexServiceImpl.class)
    private static final int IDX_STAS_NORMAL = 0
    private static final int IDX_STAS_UPDATE = 1
    private static final int IDX_STAS_ERROR = 2
    static final String KEY = "Key"
    static final String Value = "Value"
    static final String OPER_TYPE_UPDATE = "update"
    static final String OPER_TYPE_SAVE = "save"
    public static List dsNms = null
    static List m_IncludeDS = null
    static List m_ExcludeDS = null
    def private BEG_TIME = "beginDateTime"
    def private END_TIME = "endDateTime"
    @Autowired
    RawSQLService rawSQLService
    @Autowired
    XmlRawSQLService xmlRawSQLService
    @Autowired
    DBCommon_mysql dbCommon_mysql

    //邮件服务
    @Autowired
    MailService mailService
    @Autowired
    EbeanServerPool ebeanServerPool;

    //全量更新
    @Override
    void totalSync() {
        logger.info("指标数据初始化开始")
        long startTime = System.currentTimeMillis();
        //创建索引
        def cols = [KEY] as String[];
        dbCommon_mysql.createIndex(MongoDBConfigConstants.INDEX_ALL_DB, [cols]);
        //查询全部的指标数据
        List<Map<String, Object>> indexLst = xmlRawSQLService.queryRawSqlByKey("selectIndexAll");
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
            PropertiesUtil.writeProperties(PropertiesUtil.LAST_SCAN_TIME_FILE,PropertiesUtil.LAST_SCAN_TIME, DateUtils.date2String(new Date(), DateUtils.FORMAT_DATETIME))
        }
        Map<String, Map<String, Date>> dsTimeMap = [:]
        Map<String, String> timeMap = getScanTime()
        // 过滤指标结果集
        execFirInsLst(indexLst, OPER_TYPE_SAVE, dsTimeMap, timeMap)

        long endTime = System.currentTimeMillis();
        logger.info("指标数据初始化结束,耗时" + (endTime - startTime) + "ms")
        mailService.sendMail("指标数据完成初始化扫描", "【指标数据】初始化完成,共耗时：" + (endTime - startTime) + "ms");
    }

    //增量跟新
    @Override
    void incrementalSync() {
        logger.info("指标数据增量更新开始")
        long startTime = System.currentTimeMillis()

        Map timeMap = getScanTime()
        Map<String, Map<String, Date>> dsTimeMap = new HashMap<String, Map<String, Date>>();
        Map<String, Map<String, Object>> tblTimeMap = new HashMap<String, Map<String, Object>>();
        //从数据库更新
        List<Map<String, Object>> dataSrcs = getChgTbls(timeMap, tblTimeMap, dsTimeMap)
        //过滤不需要处理的数据源
        dataSrcs = getNeedScanList(dataSrcs)
        //保存全部的数据源名称
        List<String> dsNmLst = []
        dataSrcs.each {
            dsNmLst.add(it.DS_ENG_NAME)
        }
        //处理指定数据源
        if (dsNms != null && dsNms.size() > 0) {
            logger.debug("制定的数据源为:" + dsNms.toArray())
            for (int i = 0; i < dsNmLst.size(); i++) {
                if (!dsNms.contains(dsNmLst.get(i))) {
                    dsNmLst.remove(i)
                    i--
                }
            }
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
        logger.debug("本次执行-U操作的数据源信息为:" + indexLst.toArray())
        execFirInsLst(indexLst, OPER_TYPE_UPDATE, dsTimeMap, timeMap)
        long endTime = System.currentTimeMillis();
        logger.info("指标数据增量更新结束,耗时" + (endTime - startTime) + "ms")
    }

    //指标源属性变更后更新
    public void UpdateIdxProChgToMgDB() {
        if (PropertiesUtil.getProperty("index.prochg") == "false") {
            return
        }
        String lastIdxChgStartDate = PropertiesUtil.getProperty(PropertiesUtil.LAST_SCAN_TIME_FILE,PropertiesUtil.LAST_IDX_CHG_DATE)
        String lastIdxChgEndDate = DateUtils.date2String(new Date(), DateUtils.FORMAT_DATE)

        //指标列表
        List indexLst = xmlRawSQLService.queryRawSqlByKey("selectIdxByUdpTime",
                ["beginDateTime": lastIdxChgStartDate, "endDateTime": lastIdxChgEndDate])
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
        List<Map<String, Object>> dataSrcs = xmlRawSQLService.queryRawSqlByKey("selectMarcoDataSrc")
        for (dds in dataSrcs) {
            def dsName = dds.DS_ENG_NAME
            try {
                logger.debug("宏观指标【" + dsName + "】开始")
                long start = System.currentTimeMillis();
                List props = xmlRawSQLService.queryRawSqlByKey("selectDopByObjId", [obj_id: dds.OBJ_ID])
                Date oldUpdTime = getMacroMaxUpdTime(dsName);

                AllIndexMacroDsResultHandler dsRow = new AllIndexMacroDsResultHandler(dbCommon_mysql, props, oldUpdTime, dsName, mailService);
                String sql = StringUtils.ClobToString(dds.SQL_CLAUSE) + " where UPD_TIME >= TO_DATE('" + DateUtils.date2String(oldUpdTime, DateUtils.FORMAT_DATETIME) + "', " + "'YYYY-MM-DD HH24:MI:SS')" + " order by UPD_TIME asc";
                executeHandler(dds.db_name, dsRow, sql, null, dsName);

                /*  rawSQLService.queryRawSql(StringUtils.ClobToString(dds.SQL_CLAUSE) + " where UPD_TIME >= TO_DATE('" +
                          DateUtils.date2String(oldUpdTime, DateUtils.FORMAT_DATETIME) + "', " +
                          "'YYYY-MM-DD HH24:MI:SS')" + " order by UPD_TIME asc", dsRow)*/
                long end = System.currentTimeMillis();
                logger.debug("宏观指标【" + dsName + "】结束：耗时：" + (end - start) + "ms")
            } catch (err) {
                logger.error("宏观指标【" + dsName + "】更新失败：" + err)
                mailService.sendMail("宏观指标" + dsName + "更新失败", "宏观指标【" + dsName + "】更新失败，错误信息：" + err)
            }
        }
        long endTime = System.currentTimeMillis();
        logger.info("宏观指标更新结束，耗时：" + (endTime - startTime) + "ms");
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
        List indexLst = xmlRawSQLService.queryRawSqlByKey("selectIndexAll")
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
            def list = dbCommon_mysql.findByWhereSql(MongoDBConfigConstants.INDEX_ALL_DB, "`Key` like '" + lastScanIdx + "_%'");
            logger.info("scan idx  id-" + lastScanIdx + ": start")
            if (!indexIdxLst.contains(lastScanIdx)) {
                for (var in list) {
                    dbCommon_mysql.delete(MongoDBConfigConstants.INDEX_ALL_DB, ["KEY": var.get("Key")]);
                    delCount++
                }
                logger.debug("delete dirty index " + pattern + ":" + delCount);
            } else {
                String idxParam = idxMap.get(lastScanIdx).get("IDX_PARAM")
                // 带有参数的情况下
                ObjectMapper mapper = new ObjectMapper();
                // 指标参数
                ParamObj[] result = mapper.readValue(idxParam, ParamObj[].class)
                for (var in cur) {
                    String keyStr = var.get("Key")
                    // IDX_参数 = 参数 + 1
                    if (keyStr.split("_").length != result.length + 1) {
                        dbCommon_mysql.delete(MongoDBConfigConstants.INDEX_ALL_DB, ["KEY": var.get("Key")]);
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
        logger.info("定时删除无效指标结束，耗时：" + (endTime - startTime) + "ms")
    }
    /**
     * 获取宏观指标更新开始时间
     * @param idMarcoTemplate
     * @param dsNm
     * @return
     */
    private Date getMacroMaxUpdTime(String dsNm) {
        Date oldUpdTime = DateUtils.string2Date("1900-01-01", DateUtils.FORMAT_DATE)
        def list = dbCommon_mysql.find(MongoDBConfigConstants.IDX_MARCO_UPD, ["DS_NAME": dsNm]);
        if (list != null && list.size() > 0) {
            Map old = (Map) list.get(0);
            if (old != null) {
                oldUpdTime = (Date) old.get("MAX_UPD_TIME");
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
    List<Map<String, Object>> getChgTbls(Map<String, String> dateTimeMap,
                                         Map<String, Map<String, Object>> tblTimeMap,
                                         Map<String, HashMap<String, Date>> dsTimeMap) {
        List<Map<String, Object>> tbls = xmlRawSQLService.queryRawSqlByKey("selectAllTbl")
        //保存数据源名称
        List<String> needTbls = new ArrayList<String>();
        //保存需要更新的数据源信息
        List<Map<String, Object>> dsLst = new ArrayList<Map<String, Object>>();
        //去除重复的数据源名称
        List<String> dsNms = new ArrayList<String>();
        tbls.each {
            if ("DUAL".equalsIgnoreCase(it.TBL_NAME)) {
                //数据源中心库涉及到的DUAL表过滤掉
            } else if (!needTbls.contains(it.TBL_NAME)) {
                needTbls.add(it.TBL_NAME);
                try {
                    String sql = 'select max(Upd_Time) UPD_TIME from ' + it.TBL_NAME
                    String updateTimeFiled = "Upd_Time"
                    if (org.apache.commons.lang.StringUtils.isNotEmpty(PropertiesUtil.getProperty(MongoDBConfigConstants.DS_CENTER_TB_UPD_COULMN))) {
                        updateTimeFiled = PropertiesUtil.getProperty(MongoDBConfigConstants.DS_CENTER_TB_UPD_COULMN)
                        sql = sql.replace("Upd_Time", updateTimeFiled)
                    }
                    List<Map<String, Object>> result = rawSQLService.queryRawSql(sql)
                    Date maxTblDate = null;
                    if (result != null && result.size() > 0) {
                        maxTblDate = (Date) result.get(0).updateTimeFiled
                        // 保存各表的最新更新时间
                        tblTimeMap.put(it.TBL_NAME, ["TBL_NAME": it.TBL_NAME, "UPD_TIME": maxTblDate]);
                    }
                    Date beginDate = DateUtils.string2Date(dateTimeMap.beginDateTime, DateUtils.FORMAT_DATETIME);
                    Date lastTblUpdTime = beginDate;
                    // 判断最新数据时间是否在扫描时间周期内
                    logger.debug(" it.TBL_NAME:" + it.TBL_NAME + "  maxUpdateTime: " + maxTblDate + "   lastTblUpdTime:" + lastTblUpdTime)
                    if (maxTblDate != null && maxTblDate.compareTo(lastTblUpdTime) > 0) {
                        List<Map<String, Object>> dataSrcs = xmlRawSQLService.queryRawSqlByKey("selectDataSrcByTbl", [TBL_NAME: it.TBL_NAME])

                        // 获取每个源上次更新时间及设置本次更新时间
                        for (Map<String, Object> dds : dataSrcs) {
                            String dsNm = dds.DS_ENG_NAME;
                            // 源的更新时间为空则使用表的更新填充
                            if (dsTimeMap.get(dsNm) == null) {
                                dsTimeMap.put(dsNm, ["beginDateTime": lastTblUpdTime, "endDateTime": maxTblDate])
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
                } catch (err) {
                    logger.error(err)
                }
            }
        }
        return dsLst
    }

    /**
     * 根据数据源过滤指标
     * @param indexLst
     * @param operType
     * @param dsTimeMap
     * @param timeMap
     */
    void execFirInsLst(List<Map<String, Object>> indexLst, String operType, Map<String, Map<String, Date>> dsTimeMap, Map<String, String> timeMap) {
        //处理过的指标id存储的位置
        List indexNmLst = []
        //保存数据源对应的指标
        List<Map<String, Object>> wtExeInsLst = []
        def dsEngName = ""
        //创建线程池
        ThreadPoolExecutor producerPool = new ThreadPoolExecutor(20, 20, 3, TimeUnit.MINUTES, new LinkedBlockingQueue(),
                new ThreadPoolExecutor.CallerRunsPolicy())
        //循环遍历指标进行数据处理
        for (indexObj in indexLst) {
            if (indexNmLst.contains(indexObj.IDX_ID)) {
                continue
            }
            indexNmLst.add(indexObj.IDX_ID)
            // 记录上次的数据源名称,不同则一组指标更新
            if (dsEngName != "" && dsEngName != indexObj.DS_ENG_NAME) {
                Map<String, Date> dsMap = dsTimeMap.get(dsEngName)
                if (dsMap == null) {
                    dsMap = [:]
                    dsMap.put(BEG_TIME, DateUtils.string2Date(timeMap.get(BEG_TIME), DateUtils.FORMAT_DATETIME))
                    dsMap.put(END_TIME, DateUtils.string2Date(timeMap.get(END_TIME), DateUtils.FORMAT_DATETIME))
                    dsTimeMap.put(dsEngName, dsMap)
                }
                producerPool.execute(new ThreadPoolTaskM(wtExeInsLst, operType, dsTimeMap.get(dsEngName)));
                wtExeInsLst = []
            }
            wtExeInsLst.add(indexObj)
            dsEngName = indexObj.DS_ENG_NAME
        }
        //循环外再做一次
        if (wtExeInsLst.size() != 0) {
            Map<String, Date> dsMap = dsTimeMap.get(dsEngName)

            if (dsMap == null) {
                dsMap = [:]
                dsMap.put(BEG_TIME, DateUtils.string2Date(timeMap.get(BEG_TIME), DateUtils.FORMAT_DATETIME))
                dsMap.put(END_TIME, DateUtils.string2Date(timeMap.get(END_TIME), DateUtils.FORMAT_DATETIME))
                dsTimeMap.put(dsEngName, dsMap)
            }
            producerPool.execute(new ThreadPoolTaskM(wtExeInsLst, operType, dsTimeMap.get(dsEngName)))
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
        private List<Map<String, Object>> wtExeInsLst
        private String operType
        private Map<String, Date> timeMap

        public ThreadPoolTaskM(List<Map<String, Object>> wtExeInsLst, String operType, Map<String, Date> timeMap) {
            this.wtExeInsLst = wtExeInsLst
            this.operType = operType
            this.timeMap = timeMap
        }

        @Override
        public void run() {
            try {
                if (operType == OPER_TYPE_SAVE) {
                    logger.info("指标数据源【" + wtExeInsLst.get(0).DS_ENG_NAME + "】初始化开始");
                } else {
                    logger.info("指标数据源【" + wtExeInsLst.get(0).DS_ENG_NAME + "】更新开始");
                }

                long start = System.currentTimeMillis();
                // 从中心数据库更新
                execAllIdxFormDB(wtExeInsLst, operType, timeMap);
                long end = System.currentTimeMillis();
                if (operType == OPER_TYPE_SAVE) {
                    logger.info("指标数据源【" + wtExeInsLst.get(0).DS_ENG_NAME + "】初始化结束，耗时：" + (end - start) + "ms");
                } else {
                    logger.info("指标数据源【" + wtExeInsLst.get(0).DS_ENG_NAME + "】更新结束，耗时：" + (end - start) + "ms");
                }
            } catch (Exception e) {
                updateIdxStatus(wtExeInsLst.get(0).DS_ENG_NAME, (Long) wtExeInsLst.get(0).DS_OBJ_ID, (Long) wtExeInsLst.get(0).DS_TYP, IDX_STAS_ERROR)
                if (operType == OPER_TYPE_SAVE) {
                    logger.error("指标数据源【" + wtExeInsLst.get(0).DS_ENG_NAME + "】初始化失败：" + e.toString());
                    mailService.sendMail("指标数据源" + wtExeInsLst.get(0).DS_ENG_NAME + "初始化失败", "指标数据源【" + wtExeInsLst.get(0).DS_ENG_NAME + "】初始化失败，错误信息：" + e)
                } else {
                    logger.error("指标数据源【" + wtExeInsLst.get(0).DS_ENG_NAME + "】更新失败：" + e.toString());
                    mailService.sendMail("指标数据源" + wtExeInsLst.get(0).DS_ENG_NAME + "增量更新失败", "指标数据源【" + wtExeInsLst.get(0).DS_ENG_NAME + "】增量更新失败，错误信息：" + e)
                }
            }
        }
    }

    /**
     * 从关系型数据库更新指标数据
     * @param wtExeInsLstTmp
     * @param operType
     * @param scanTime
     */
    private void execAllIdxFormDB(List<Map<String, Object>> wtExeInsLst, String operType, Map<String, Date> scanTime) {
        List<Map<String, Object>> wtExeInsLstTmp = new ArrayList<Map<String, Object>>();
        wtExeInsLstTmp.addAll(wtExeInsLst)
        if (wtExeInsLstTmp.size() == 0) {
            return
        }
        Map<String, Object> indexMap = wtExeInsLstTmp.get(0)
        List<Map<String, Object>> props = xmlRawSQLService.queryRawSqlByKey("selectDopByObjId", [obj_id: indexMap.DS_OBJ_ID])
        updateIdxStatus(indexMap.DS_ENG_NAME, (Long) indexMap.DS_OBJ_ID, (Long) indexMap.DS_TYP, IDX_STAS_UPDATE)
        logger.debug("本次数据源类型哦：ds_typ:" + indexMap.get("ds_typ"))
        // 数据源是否带有参数
        if (indexMap.get("ds_typ") == 2 || (indexMap.get("ds_typ") == 6 && (indexMap.get("ds_param") == null
                || "" == indexMap.get("ds_param")))) {
            saveIdxFromDb(wtExeInsLstTmp, props, StringUtils.ClobToString(indexMap.get("SQL_CLAUSE")), null)
        } else if (indexMap.get("ds_typ") == 3) {
            // 带有参数的情况下  json格式解析成javaBean
            ObjectMapper mapper = new ObjectMapper()
            ParamObj[] result = mapper.readValue(indexMap.PARAM_DFT_VAL, ParamObj[].class)
            // 按照参数名称字母排序组成mongoDB集合名称
            Collections.sort(Arrays.asList(result))
            // 设置各参数类型
            Map paramMap = [:]
            // 数据源加入参数的SQL
            result.each {
                //获取各自参数的类型
                List<Map<String, Object>> paramTypes = xmlRawSQLService.queryRawSqlByKey("selectParamType", [param_code: it.type])
                if (paramTypes != null && paramTypes.size() > 0) {
                    paramMap.put(it.name, paramTypes.get(0).param_typ);
                }
            }

            //全量数据
            if (operType == OPER_TYPE_SAVE) {
                String sql = replaceSql(StringUtils.ClobToString(indexMap.SQL_CLAUSE), result);
//                List<Map<String, Object>> piecewiseList = rawSQLService.queryRawSql(indexMap.DS_PARAM_FULL);
                List<Map<String, Object>> piecewiseList = executeSqlByDbName(indexMap.db_name, indexMap.DS_PARAM_FULL, null, indexMap.DS_ENG_NAME)
                for (Map<String, Object> obj : piecewiseList) {
                    //设置参数值
                    Map<String, Object> sqlParams = new HashMap<String, Object>();
                    paramMap.each { key, value ->
                        if (value == 10) {
                            sqlParams.put(key, Long.valueOf(obj.get(key) + ""))
                        } else if (value == 20) {
                            sqlParams.put(key, obj.get(key) + "")
                        } else if (value == 30) {
                            sqlParams.put(key, obj.get(key))
                        }
                    }
                    //保存数据
                    saveIdxFromDb(wtExeInsLstTmp, props, sql, sqlParams);
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
                List<Map<String, Object>> piecewiseList = executeSqlByDbName(indexMap.db_name, updParamsql, sqlParam, indexMap.DS_ENG_NAME)
                for (Map<String, Object> obj : piecewiseList) {
                    //设置参数值
                    Map<String, Object> sqlParams = new HashMap<String, Object>();
                    paramMap.each { key, value ->
                        if (value == 10) {
                            sqlParams.put(key, Long.valueOf(obj.get(key) + ""))
                        } else if (value == 20) {
                            sqlParams.put(key, obj.get(key) + "")
                        } else if (value == 30) {
                            sqlParams.put(key, obj.get(key))
                        }
                    }
                    //保存数据
                    saveIdxFromDb(wtExeInsLstTmp, props, sql, sqlParams);
                }
            }
        } else if (indexMap.get("ds_typ") == 7) {
            /*ObjectMapper mapper = new ObjectMapper()
            ParamObj[] result = mapper.readValue(StringUtils.ClobToString(indexMap.UPD_KEY), ParamObj[].class)
            // 按照参数名称字母排序组成mongoDB集合名称
            Collections.sort(Arrays.asList(result))
            // 增量参数列表名称
            List params = []
            Map paramMap = [:]
            for (int i = 0; i < result.length; i++) {
                params.add(result[i].getName())
                // 获取各自参数的类型
                def paramTypes = xmlRawSQLService.queryRawSqlByKey("selectParamType", [param_code: result[i].getType()])
                paramMap.put(result[i].getName(), (Long) paramTypes[0].get("param_typ"))
            }
            if (operType == OPER_TYPE_SAVE) {
                String dsSql = StringUtils.ClobToString(indexMap.get("init_sql"))
                saveIdxFromDb(wtExeInsLstTmp, props, dsSql, null)
            } else if (operType == OPER_TYPE_UPDATE) {
                String sql = replaceSql(StringUtils.ClobToString(indexMap.SQL_CLAUSE), result);
                // 获取更新参数的sql
                String updParamsql = StringUtils.ClobToString(indexMap.get("UPD_PARAM"))
                updParamsql = replaceSql(updParamsql, [new ParamObj(name: "BGN_TIME"), new ParamObj(name: "END_TIME")] as ParamObj[])
                //查询分段数据
                Map<String, Object> sqlParam = new HashMap<String, Object>();
                sqlParam.put("BGN_TIME", DateUtils.date2String(scanTime.beginDateTime, DateUtils.FORMAT_DATETIME))
                sqlParam.put("END_TIME", DateUtils.date2String(scanTime.endDateTime, DateUtils.FORMAT_DATETIME))
                //获取更新时间段
//                List<Map<String, Object>> incrementalList = rawSQLService.queryRawSql(updParamsql, sqlParam);
                List<Map<String, Object>> incrementalList = executeSqlByDbName(indexMap.db_name, updParamsql, sqlParam, indexMap.DS_ENG_NAME)
                for (Map<String, Object> obj : incrementalList) {
                    //设置参数值
                    Map<String, Object> sqlParams = new HashMap<String, Object>();
                    paramMap.each { key, value ->
                        if (value == 10) {
                            sqlParams.put(key, Long.valueOf(obj.get(key) + ""))
                        } else if (value == 20) {
                            sqlParams.put(key, obj.get(key) + "")
                        } else if (value == 30) {
                            sqlParams.put(key, obj.get(key))
                        }
                    }
                    //保存数据
                    saveIdxFromDb(wtExeInsLstTmp, props, sql, sqlParams);
                }
            }*/
        }
        // 结束操作更新状态
        updateIdxStatus(indexMap.DS_ENG_NAME, (Long) indexMap.DS_OBJ_ID, (Long) indexMap.DS_TYP, IDX_STAS_NORMAL)
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

    /**
     * 修改最新更新时间
     * @param dsNm
     * @param updTime
     * @param dbNm
     */
    private void writeDsUpdTimeToMg(String dsNm, Date updTime, String dbNm) {
        List list = dbCommon_mysql.find(dbNm, ["DS_NAME": dsNm]);
        Map dsStatus = list != null ? list.get(0) : [:];
        if (null != dsStatus) {
            dsStatus.put("LAST_UPD_TIME", updTime);
            dbCommon_mysql.update(dbNm, dsStatus, ["DS_NAME": dsNm]);
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
        List list = dbCommon_mysql.find(MongoDBConfigConstants.IDX_STATUS_DB, ["DS_NAME": dsName]);
        Map dsStatus = list != null && list.size() > 0 ? list.get(0) : [:];
        if (null == dsStatus || dsStatus.size() == 0) {
            dsStatus = addIdxStatus(dsName, dsObjId, dsTyp)
            dsStatus.put("DS_STAS", status)
            dbCommon_mysql.insert(MongoDBConfigConstants.IDX_STATUS_DB, dsStatus);
        } else {
            dsStatus.put("DS_STAS", status)
            dsStatus.put("DS_UPD_TIME", new Date())
            dbCommon_mysql.update(MongoDBConfigConstants.IDX_STATUS_DB, dsStatus, ["DS_NAME": dsName]);
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
        return dsStatus
    }

    /**
     * 从关系型数据库保存指标数据
     * @param wtExeInsLst
     * @param props
     * @param sql
     * @param operType
     */
    private void saveIdxFromDb(List<Map<String, Object>> wtExeInsLst, List<Map<String, Object>> props, String sql, Map<String, Object> sqlParams) {
        // 带有参数的情况下
        ObjectMapper mapper = new ObjectMapper()
        ParamObj[] result = mapper.readValue(wtExeInsLst.get(0).get("idx_param"), ParamObj[].class)
        // 按照参数名称字母排序组成mongoDB集合名称
        Collections.sort(Arrays.asList(result))
        Map<String, Map<String, Object>> propsMap = getProsMap(props)
        AllIndexFrDBResultHandler dsRow = new AllIndexFrDBResultHandler(result, propsMap, wtExeInsLst, dbCommon_mysql, mailService)
        //判断是否有sql参数
        if (sqlParams != null) {
//            rawSQLService.queryRawSql(sql,sqlParams,dsRow)
            executeHandler(wtExeInsLst.get(0).get("db_name"), dsRow, sql, sqlParams, wtExeInsLst.get(0).get("DS_ENG_NAME"));
        } else {
//            rawSQLService.queryRawSql(sql, dsRow)
            executeHandler(wtExeInsLst.get(0).get("db_name"), dsRow, sql, null, wtExeInsLst.get(0).get("DS_ENG_NAME"));
        }
    }

    /**
     * 获取扫描的时间段
     * @return
     */
    def getScanTime() {
        Map dateTimeMap = [:]
        def beginDateTime;
        def endDateTime;
        String lastScanTimeStr = PropertiesUtil.getProperty(PropertiesUtil.LAST_SCAN_TIME_FILE,PropertiesUtil.LAST_SCAN_TIME)
        Date date = new Date()

        if (lastScanTimeStr == null || lastScanTimeStr == "") {
            endDateTime = DateUtils.date2String(date, DateUtils.FORMAT_DATETIME);
            beginDateTime = DateUtils.date2String(DateUtils.addMinutes(date, -10), DateUtils.FORMAT_DATETIME);
        } else {
            endDateTime = DateUtils.date2String(date, DateUtils.FORMAT_DATETIME);
            beginDateTime = lastScanTimeStr
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
    private Map<String, Map<String, Object>> getProsMap(List<Map<String, Object>> props) {
        Map<String, Map<String, Object>> map = [:]
        props.each {
            map.put(it.PROP_NAME, it)
        }
        return map
    }

    /**
     * 过滤数据源
     * @param dsname
     * @return
     */
    private List getNeedScanList(List<Map<String, Object>> dsLst) {
        List<Map<String, Object>> result = []
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
    private boolean isNeedScan(String dsname) {
        if (m_ExcludeDS != null && m_ExcludeDS.size() > 0) {
            return !m_ExcludeDS.contains(dsname.toUpperCase())
        }
        if (m_IncludeDS != null && m_IncludeDS.size() > 0) {
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

    /***
     * 根据不同的db_name,执行handler
     */
    private void executeHandler(String db_name, ResultHandler resultHandler, String sql, Map params, String ds_name) {
        String dbName = db_name == null ? "default" : db_name;
        EbeanServer dbEbeanServer = null;
        try {
            dbEbeanServer = ebeanServerPool.getByName(dbName);
        } catch (e) {
            dbEbeanServer = ebeanServerPool.getByName("default");
            logger.error("指标数据源【" + ds_name + "】获取数据连接失败，将使用默认连接尝试获取数据,错误信息：" + e);
        }
        try {
            logger.debug("executeHandler(): 保存数据源【" + ds_name + "】的数据，获取数据源的数据连接。----" + dbName);
            SqlQuery query = dbEbeanServer.createSqlQuery(sql);
            if (params) {
                params.each { key, value ->
                    query.setParameter(key, value)
                }
            }
            query.findEach(new Consumer<SqlRow>() {
                @Override
                public void accept(SqlRow row) {
                    resultHandler.execute(row)
                }
            })
        } catch (err) {
            logger.error("指标数据源【" + ds_name + "】执行sql错误：---" + err);
        }
    }

    /***
     * 根据不同的db_name,连接不同的数据库执行sql
     */
    private List<Map<String, Object>> executeSqlByDbName(String db_name, String sql, Map params, String ds_name) {
        String dbName = db_name == null ? "default" : db_name;
        EbeanServer dbEbeanServer = null;
        try {
            dbEbeanServer = ebeanServerPool.getByName(dbName);
        } catch (e) {
            dbEbeanServer = ebeanServerPool.getByName("default");
            logger.error("指标数据源【" + ds_name + "】获取数据连接失败，将使用默认连接尝试获取数据,错误信息：" + e);
        }
        try {
            logger.debug("executeSqlByDbName(): 连接【" + ds_name + "】执行sql。----" + dbName);
            SqlQuery query = dbEbeanServer.createSqlQuery(sql);
            if (params) {
                params.each { key, value ->
                    query.setParameter(key, value)
                }
            }
            return query.findList();
        } catch (err) {
            logger.error("指标数据源【" + ds_name + "】执行sql错误：---" + err);
        }
        return null
    }

}
