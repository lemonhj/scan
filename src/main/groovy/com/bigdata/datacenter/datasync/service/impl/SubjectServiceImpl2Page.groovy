package com.bigdata.datacenter.datasync.service.impl

import com.avaje.ebean.EbeanServer
import com.avaje.ebean.SqlQuery
import com.avaje.ebean.SqlRow
import com.bigdata.datacenter.datasync.core.data.MongoTemplatePool
import com.bigdata.datacenter.datasync.model.mongodb.ParamObj
import com.bigdata.datacenter.datasync.model.mongodb.SubjectStatus
import com.bigdata.datacenter.datasync.quartz.SubjectServiceJob
import com.bigdata.datacenter.datasync.service.ScanService
import com.bigdata.datacenter.datasync.service.SubjectStatusService
import com.bigdata.datacenter.datasync.service.TxtAndRrpStatusService
import com.bigdata.datacenter.datasync.service.XmlRawSQLService
import com.bigdata.datacenter.datasync.utils.DateUtils
import com.bigdata.datacenter.datasync.utils.MongoUtils
import com.bigdata.datacenter.datasync.utils.PropertiesUtil
import com.bigdata.datacenter.datasync.utils.StringUtils
import com.bigdata.datacenter.datasync.utils.constants.MongoDBConfigConstants
import com.bigdata.datacenter.metadata.data.EbeanServerPool
import com.bigdata.datacenter.metadata.service.RawSQLService
import com.bigdata.datacenter.metadata.service.ResultHandler
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.log4j.Logger
import org.quartz.*
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.data.mongodb.core.MongoOperations
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.stereotype.Service

import java.sql.SQLException
import java.text.ParseException
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.function.Consumer

/**
 * 专题数据源
 * Created by qq on 2017/5/19.
 */
@Service(value = "subject-page")
class SubjectServiceImpl2Page implements ScanService {
    private static final Logger logger = Logger.getLogger(SubjectServiceImpl2Page.class)
    @Autowired
    SubjectServiceImpl2Page subjectServiceImpl2Page
    @Autowired
    Scheduler scheduler
    @Autowired
    RawSQLService rawSQLService
    @Autowired
    XmlRawSQLService xmlRawSQLService
    @Autowired
    @Qualifier("subjectStatusServiceImpl")
    SubjectStatusService subjectStatusService
    @Autowired
    MongoTemplatePool mongoTemplatePool
    MongoOperations subjectTemplate;
    static List<String> dsNms = null;
    // 周期类型的job
    public static final String PRID = "PRID";
    // 手动立即触发的job
    public static final String IMID = "IMID";

    private ThreadPoolExecutor producerPool = null;

    @Autowired
    EbeanServerPool ebeanServerPool;

    //邮件服务
    @Autowired
    MailService mailService

    void resetDsStats(){
        //重置数据源状态，将状态1全部改成状态0
        subjectStatusService.resetDsStatus(1,0);
    }
    /**
     * 增量更新
     */
    @Override
    void incrementalSync() {
        logger.info("专题数据源增量更新开始")
        long startTime = System.currentTimeMillis();
        //获取更新时间
        Map<String, String> dateTimeMap = getLastScanTime();
        try {
            producerPool = new ThreadPoolExecutor(10, 10, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(),
                    new ThreadPoolExecutor.CallerRunsPolicy());

            // 修改数据源属性变更
            if (PropertiesUtil.getProperty("sub.prochg").equals("true")) {
                updateDsProChg(dateTimeMap);
            }
            // 删除不存在的数据源
            try{
                delNoUseDs();
            }catch (e){
                logger.error("[专题数据源增量更新]删除无效数据失败,错误信息："+e)
            }

            //追加新的专题数据源
            appendNewSubToMgDB()

            //获取需要更新的数据源信息
            Map<String, Map<String, Date>> dsTimeMap = new HashMap<String, Map<String, Date>>();
            Map<String, Map<String, Object>> tblTimeMap = new HashMap<String, Map<String, Object>>();
            List<Map<String, Object>> dataSrcs = getChgTbl(MongoDBConfigConstants.SUB_TBL_UPD_TIME, dateTimeMap, tblTimeMap, dsTimeMap);
            for (Map<String, Object> dds : dataSrcs) {
                if (dds.DS_ENG_NAME.equals("ENTERPRISEBASICDATA_1")) {
                    continue;
                }
                Map<String, Date> tmMap = dsTimeMap.get(dds.DS_ENG_NAME);
                Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                    public void uncaughtException(Thread t, Throwable e) {
                        logger.error("["+dds.DS_ENG_NAME + "] 增量更新错误信息:" + e);
                    }
                });
                producerPool.execute(new ThreadPoolTask(dds,"update",tmMap))
//                updateSubObj(dds, tmMap);
            }
            try {
                if (producerPool != null) {
                    producerPool.shutdown();
                    while (!producerPool.awaitTermination(1, TimeUnit.SECONDS)) {
                    }

                    //记录扫描时间
                    PropertiesUtil.writeProperties(PropertiesUtil.LAST_SCAN_TIME_FILE,"sub.lastScanTime", dateTimeMap.endDateTime)
                    // 写入每个表的更新时间
                    saveTblTimeMapToMg(tblTimeMap, MongoDBConfigConstants.SUB_TBL_UPD_TIME);
                }
            } catch (e) {
                logger.error("====专题数据源增量更新错误 :" + e);
            }
        } catch (err) {
            logger.error("专题数据源定時同步更新错误：" + err);
            //记录扫描时间
            PropertiesUtil.writeProperties(PropertiesUtil.LAST_SCAN_TIME_FILE,"sub.lastScanTime", dateTimeMap.beginDateTime)
        }
        long endTime = System.currentTimeMillis();
        logger.info("专题数据源增量更新完成,共耗时：" + (endTime - startTime) + "ms");
    }

    /**
     * 全量更新
     */
    @Override
    void totalSync() {
        logger.info("专题数据源全量更新开始")
        long startTime = System.currentTimeMillis();
        try {
            producerPool = new ThreadPoolExecutor(20, 20, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(),
                    new ThreadPoolExecutor.CallerRunsPolicy());

            List<Map<String, Object>> dataSrcs = xmlRawSQLService.queryRawSqlByKey("selectAllDataSrc")
            dataSrcs.each() {
                //在进行初始化之前对状态进行修改
                subjectStatusService.updateDsStatus(it, TxtAndRrpStatusService.DS_STAS_NORMAL, null);

                //判断是否单数据源操作
                if (dsNms != null && dsNms.size() > 0) {
                    if (dsNms.contains(it.DS_ENG_NAME)) {
                        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                            public void uncaughtException(Thread t, Throwable e) {
                                logger.error(it.DS_ENG_NAME + " 错误信息:" + e.toString());
                            }
                        });
                        producerPool.execute(new ThreadPoolTask(it,true,"save"))
//                        saveSubObj(it, true)
                    }
                } else {
                    //记录扫描时间
                    PropertiesUtil.writeProperties(PropertiesUtil.LAST_SCAN_TIME_FILE,"sub.lastScanTime", DateUtils.date2String(new Date(), DateUtils.FORMAT_DATETIME))
                    Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                        public void uncaughtException(Thread t, Throwable e) {
                            logger.error(it.DS_ENG_NAME + " 全量更新错误信息:" + e.toString());
                        }
                    });
                    producerPool.execute(new ThreadPoolTask(it,true,"save"))
//                    saveSubObj(it, true)
                }
            }
        } catch (err) {
            logger.error("专题数据源全量更新错误：" + err);
        }  finally {
            try{
                if (producerPool != null) {
                    producerPool.shutdown();
                    while (!producerPool.awaitTermination(1, TimeUnit.SECONDS)) {
                    }
                }
            }catch (e){
                logger.error("专题数据源全量更新错误 :" + e.toString());
            }
        }
        long endTime = System.currentTimeMillis();
        logger.info("专题数据源全量更新完成,共耗时：" + (endTime - startTime) + "ms");
    }

    /**
     * 强制手动刷新数据更新到mongoDB
     */
    void saveImedSubToShdle() {
        logger.info("强制手动刷新专题数据开始")
        long startTime = System.currentTimeMillis();
        try {
            // 获取sqlMapper
            List<Map<String, Object>> dataSrcs = xmlRawSQLService.queryRawSqlByKey("selectImedDataSrc")
            if (!schedulerNew.isStarted()) {
                schedulerNew.start();
            }

            if (dataSrcs != null && dataSrcs.size() > 0) {
                dataSrcs.each {
                    try {
                        chkImdJobTrigger(it, schedulerNew, it.TRIG_TIME);
                    } catch (Exception e) {
                        // 发送邮件错误日志
                        logger.error("数据源[" + it.DS_ENG_NAME + "]强制手动刷新失败，错误信息:" + e);
                    }
                }
            }
        } catch (Exception e) {
            // 发送邮件错误日志
            logger.error("强制手动刷新数据源错误，"+e);
        }
        long endTime = System.currentTimeMillis();
        logger.info("强制手动刷新专题数据完成,共耗时：" + (endTime - startTime) + "ms");
    }

    /**
     * 手动强制刷新job任务设置
     * @param dataSrcs
     * @param scheduler
     * @param trigTime
     * @throws SchedulerException
     * @throws ParseException
     * @throws SQLException
     */
    void chkImdJobTrigger(Map<String, Object> dataSrcs, Scheduler scheduler, Date trigTime)
            throws SchedulerException, ParseException, SQLException {
        // job名称
        String jobName = dataSrcs.DS_ENG_NAME + "Job_Imd";
        // trigger名称
        String triggerName = dataSrcs.DS_ENG_NAME + "Trigger_Imd";
        JobDetail jobDetail = scheduler.getJobDetail(new JobKey(jobName, IMID));
        Trigger trigger = (Trigger) scheduler.getTrigger(new TriggerKey(triggerName, IMID));
        if (jobDetail == null) {
            // 创建任务如果此任务没有在队列中
            jobDetail = JobBuilder.newJob(SubjectServiceJob.class).
                    withIdentity(jobName, IMID).build();
        }
        // trigger为null 或者 trigger开始时间不符时候
        if (trigger == null || (trigger != null && trigger.getStartTime().compareTo(trigTime) != 0)) {
            // 已经存在触发器则删除
            if (trigger != null) {
                scheduler.deleteJob(jobName, IMID);
            }
            if (trigTime == null) {
                // 手工触发时间为立即启动
                trigger = TriggerBuilder.newTrigger()
                        .withIdentity(triggerName, IMID)
                        .startNow().build();
            } else {
                trigger = TriggerBuilder.newTrigger()
                        .withIdentity(triggerName, IMID)
                        .withSchedule(
                        SimpleScheduleBuilder.simpleSchedule().
                                withRepeatCount(0)
                                .withIntervalInMilliseconds(0L)
                ).build();
            }
            jobDetail.getJobDataMap().put("DS", dataSrcs);
            jobDetail.getJobDataMap().put("SubjectStatusService", subjectStatusService);
            jobDetail.getJobDataMap().put("subjectServiceImpl2Page", subjectServiceImpl2Page);
            scheduler.scheduleJob(jobDetail, trigger);
        }
    }

    /**
     * 周期刷新数据到mongoDB(数据触发=100 周期=200)
     */
    void savePridSubToShdle() {
        logger.info("周期刷新专题数据源开始")
        long startTime = System.currentTimeMillis();
        try {
            // 获取sqlMapper
            List<Map<String, Object>> dataSrcs = xmlRawSQLService.queryRawSqlByKey("selectPeriodDataSrc")
            if (!schedulerNew.isStarted()) {
                schedulerNew.start();
            }
            if (dataSrcs != null && dataSrcs.size() > 0) {
                dataSrcs.each {
                    try {
                        chkPridJobTrigger(it, schedulerNew);
                    } catch (Exception e) {
                        // 发送邮件错误日志
                        logger.error("数据源[" + it.DS_ENG_NAME + "]周期刷新失败，错误信息:" + e);
                    }
                }
            }
        } catch (Exception e) {
            // 发送邮件错误日志
            logger.error("周期刷新数据源错误，"+e);
        }
        long endTime = System.currentTimeMillis();
        logger.info("周期刷新专题数据完成,共耗时：" + (endTime - startTime) + "ms");
    }

    void chkPridJobTrigger(Map<String, Object> dataSrcs, Scheduler scheduler) throws SchedulerException, ParseException {
        String dsNm = dataSrcs.DS_ENG_NAME;
        // job名称
        String jobName = dsNm + "Job_Prid";
        // trigger名称
        String triggerName = dsNm + "Trigger_Prid";
        JobDetail jobDetail = scheduler.getJobDetail(new JobKey(jobName, PRID));
        String[] triggers = dataSrcs.TIME_RULE.split(";");
        boolean chgFlg = false;
        if (jobDetail == null) {
            chgFlg = true
        } else {
            List<CronTrigger> nowTriggerList = scheduler.getTriggersOfJob(new JobKey(jobName, PRID))
            List<String> trigLst = Arrays.asList(triggers);
            for (CronTrigger cronTrigger : nowTriggerList) {
                String timeRule = cronTrigger.getCronExpression();
                if (!trigLst.contains(timeRule)) {
                    chgFlg = true;
                    break;
                }
            }
        }

        // 任务已经存在但时间规则变化
        if (chgFlg) {
            scheduler.deleteJob(new JobKey(jobName, PRID));
            // 创建任务如果此任务没有在队列中
            jobDetail = JobBuilder.newJob(SubjectServiceJob.class)
                    .withIdentity(jobName, PRID)
                    .storeDurably(true).build();

            // durable, 指明任务就算没有绑定Trigger仍保留在Quartz的JobStore中,
            jobDetail.getJobDataMap().put("DS", dataSrcs);
            jobDetail.getJobDataMap().put("SubjectStatusService", subjectStatusService);
            jobDetail.getJobDataMap().put("subjectServiceImpl2Page", subjectServiceImpl2Page);
            // 加入一个任务到Quartz框架中, 等待后面再绑定Trigger,此接口中的JobDetail的durable必须为true
            scheduler.addJob(jobDetail, false);
            for (int i = 0; i < triggers.length; i++) {
                CronScheduleBuilder cronScheduleBuilder = CronScheduleBuilder.cronSchedule(triggers[i]);
                CronTrigger cronTrigger = TriggerBuilder.newTrigger()
                        .withIdentity(triggerName + "_" + i, dsNm + "_" + PRID)
                        .withSchedule(cronScheduleBuilder).build();
                scheduler.scheduleJob(jobDetail, cronTrigger);
            }
        }
    }
    /**
     * 保存专题数据源
     * @param dds
     * @param flag
     */
    void saveSubObj(Map<String, Object> dds, boolean flag) {
        def start = System.currentTimeMillis()
        try {
            // 如果这个源在更新中，则不更新
            def status = subjectStatusService.getDsStatus(dds.DS_ENG_NAME)
            if (status == TxtAndRrpStatusService.DS_STAS_UPDATE) {
                return;
            }
            logger.info("数据源[" + dds.DS_ENG_NAME + "]数据初始化开始");
            //更新状态
            subjectStatusService.updateDsStatus(dds, TxtAndRrpStatusService.DS_STAS_UPDATE, null)
            //获取sql的参数
            List<Map<String, Object>> props = xmlRawSQLService.queryRawSqlByKey("selectDopByObjId", [obj_id: dds.OBJ_ID]);
            //获取指标属性
            List<Map<String, Object>> dsIndexLst = xmlRawSQLService.queryRawSqlByKey("selectDsIndexByObjId", [obj_id: dds.OBJ_ID]);
            //静态数据源
            if (dds.DS_TYP == 2) {
                String colNm = dds.DS_ENG_NAME
                subjectTemplate = mongoTemplatePool.getByName(MongoDBConfigConstants.SUB_DATA_SOURCE)
                subjectTemplate.dropCollection(colNm + "_tmp")
                // 创建时保存索引
                MongoUtils.addIndex(subjectTemplate, colNm + "_tmp", dsIndexLst);
                //查询数据并保存到mongodb
//                rawSQLService.queryRawSql(StringUtils.ClobToString(dds.SQL_CLAUSE), new SubjectResultHandler(subjectTemplate, props, colNm + "_tmp"))
                ResultHandler resultHandler =  new SubjectResultHandler(subjectTemplate, props, colNm + "_tmp",mailService);
                executeHandler(dds.db_name,resultHandler,StringUtils.ClobToString(dds.SQL_CLAUSE),null,dds.DS_ENG_NAME);

                if (subjectTemplate.collectionExists(colNm + "_tmp")) {
                    subjectTemplate.getCollection(colNm + "_tmp").rename(colNm, true)
                }
            } else if (dds.DS_TYP == 3) {
                /**
                 * 静态分段：数据源名称作为集合名称，分段数据保存到统一集合，在分段字段添加索引加快查询效率，
                 * 这样处理能有效解决因为分段创建集合过多导致mongo服务停止的问题
                 * nijiang
                 */
                String colNm = dds.DS_ENG_NAME
                subjectTemplate = mongoTemplatePool.getByName(MongoDBConfigConstants.SUB_DATA_SOURCE)
                if (subjectTemplate.collectionExists(colNm)) { //判断数据集合是否存在，不存在则创建
                    if (flag) {   //判断是否需要删除原集合
                        subjectTemplate.dropCollection(colNm)
                    }
                }
                //判断索引列表是否为空，为空则创建
                if (dsIndexLst == null || dsIndexLst.size() == 0) {
                    dsIndexLst = new ArrayList<Map<String, Object>>();
                }
                // 获取更新参数的sql
                ParamObj[] params = new ObjectMapper().readValue(dds.PARAM_DFT_VAL, ParamObj[].class)
                // 按照参数名称字母排序组成mongoDB集合名称
                Collections.sort(Arrays.asList(params));
                // 设置各参数类型
                Map<String, Object> paramMap = new HashMap<String, Object>();
                // 数据源加入参数的SQL
                params.each {
                    // 获取各自参数的类型
                    def list = xmlRawSQLService.queryRawSqlByKey("selectParamType", [param_code: it.type]);
                    if (list != null && list.size() > 0) {
                        paramMap.put(it.name, list.get(0).param_typ);
                    }
                }
                //数据参数格式替换
                String sql = replaceSql(StringUtils.ClobToString(dds.SQL_CLAUSE), params);
                //查询分段数据
//                List<Map<String, Object>> piecewiseList = rawSQLService.queryRawSql(dds.DS_PARAM_FULL);
                List<Map<String, Object>> piecewiseList = executeSqlByDbName(dds.db_name,dds.DS_PARAM_FULL,null,dds.DS_ENG_NAME)
                for (Map<String, Object> obj : piecewiseList) {
                    //设置参数值
                    Map<String, Object> sqlParams = new HashMap<String, Object>();
                    String idxVal = ""
                    paramMap.each { key, value ->
                        if (value == 10) {
                            sqlParams.put(key, Long.valueOf(obj.get(key)+""))
                        } else if (value == 20) {
                            sqlParams.put(key, obj.get(key)+"")
                        } else if (value == 30) {
                            sqlParams.put(key, obj.get(key))
                        }
                    }
                    for(int i=0;i<params.length;i++){
                        if (paramMap.get(params[i].name) == 30) {
                            String paramDate = DateUtils.date2String(obj.get(params[i].name), DateUtils.FORMAT_DATE);
                            idxVal += "_" + paramDate
                        } else if(paramMap.get(params[i].name) == 10) {
                            idxVal += "_" + Long.valueOf(obj.get(params[i].name)+"")
                        }else {
                            idxVal += "_" + obj.get(params[i].name)
                        }
                    }

                    idxVal = idxVal.replaceFirst("_", "");
                    //删除原有数据
                    subjectTemplate.dropCollection((colNm+"-"+idxVal) + "-tmp")
                    //创建索引
                    MongoUtils.addIndex(subjectTemplate, (colNm+"-"+idxVal) + "-tmp", dsIndexLst);
                    //保存数据
//                    rawSQLService.queryRawSql(sql, sqlParams, new SubjectResultHandler(subjectTemplate, props, (colNm+"-"+idxVal) + "-tmp"))
                    ResultHandler resultHandler =  new SubjectResultHandler(subjectTemplate, props, (colNm+"-"+idxVal) + "-tmp",mailService);
                    executeHandler(dds.db_name,resultHandler,sql,sqlParams,dds.DS_ENG_NAME);
                    //将临时集合改名为正式集合
                    if (subjectTemplate.collectionExists((colNm+"-"+idxVal) + "-tmp")) {
                        subjectTemplate.getCollection((colNm+"-"+idxVal) + "-tmp").rename(colNm+"-"+idxVal, true)
                    }
                }
            } else if (dds.DS_TYP == 7) {     //增量数据源初始化
                // 获取更新参数的sql
                ParamObj[] params = new ObjectMapper().readValue(StringUtils.ClobToString(dds.UPD_KEY), ParamObj[].class)
                // 按照参数名称字母排序组成mongoDB集合名称
                Collections.sort(Arrays.asList(params));
                String colNm = dds.DS_ENG_NAME
                subjectTemplate = mongoTemplatePool.getByName(MongoDBConfigConstants.SUB_DATA_SOURCE)
                subjectTemplate.dropCollection(colNm + "_tmp")

                // 获取索引不重复属性名称
                List<String> indexFldName = new ArrayList<String>();
                dsIndexLst.each {
                    if (!indexFldName.contains(it.PROP_NAME)) {
                        indexFldName.add(it.PROP_NAME);
                    }
                }
                // 将增量更新参数加入索引队列
                List<String> paramNames = new ArrayList<String>();
                params.each {
                    paramNames.add(it.name)
                    if (!indexFldName.contains(it.name)) {
                        Map<String, Object> idx = [INDEX_NAME: it.name, PROP_NAME: it.name, ORDER_RULE: null]
                        dsIndexLst.add(idx)
                    }
                }
                // 创建时保存索引
                MongoUtils.addIndex(subjectTemplate, colNm + "_tmp", dsIndexLst);
                //查询数据并保存到mongodb
//                rawSQLService.queryRawSql(StringUtils.ClobToString(dds.INIT_SQL),
//                        new SubjectIncrementalResultHandler(subjectTemplate, props, colNm + "_tmp", paramNames, MongoDBConfigConstants.INS_TYPE))

                ResultHandler resultHandler =   new SubjectIncrementalResultHandler(subjectTemplate, props, colNm + "_tmp", paramNames, MongoDBConfigConstants.INS_TYPE,mailService);
                executeHandler(dds.db_name,resultHandler,StringUtils.ClobToString(dds.INIT_SQL),null,dds.DS_ENG_NAME);

                if (subjectTemplate.collectionExists(colNm + "_tmp")) {
                    subjectTemplate.getCollection(colNm + "_tmp").rename(colNm, true)
                }
            }
            //更新状态
            subjectStatusService.updateDsStatus(dds, TxtAndRrpStatusService.DS_STAS_NORMAL, new Date())
        } catch (err) {
            subjectStatusService.updateDsStatus(dds, TxtAndRrpStatusService.DS_STAS_ERROR, new Date())
            logger.error("数据源[" + dds.DS_ENG_NAME + "]初始化错误：" + err);
        }
        def end = System.currentTimeMillis()
        logger.info("数据源[" + dds.DS_ENG_NAME + "]数据初始化结束,共耗时：" + (end - start) + "ms");
    }

    /**
     * 修改专题数据源
     * @param dds
     * @param tmMap
     */
    void updateSubObj(Map<String, Object> dds, Map<String, Date> tmMap) {
        def start = System.currentTimeMillis()
        try {
            // 如果这个源在更新中，则不更新
            def status = subjectStatusService.getDsStatus(dds.DS_ENG_NAME)
            if (status == TxtAndRrpStatusService.DS_STAS_UPDATE) {
                return;
            }
            logger.info("数据源[" + dds.DS_ENG_NAME + "]增量更新开始");
            //更新状态
            subjectStatusService.updateDsStatus(dds, TxtAndRrpStatusService.DS_STAS_UPDATE, null)
            //获取sql的参数
            List<Map<String, Object>> props = xmlRawSQLService.queryRawSqlByKey("selectDopByObjId", [obj_id: dds.OBJ_ID]);
            //获取指标属性
            List<Map<String, Object>> dsIndexLst = xmlRawSQLService.queryRawSqlByKey("selectDsIndexByObjId", [obj_id: dds.OBJ_ID]);
            //静态数据源
            if (dds.DS_TYP == 2) {
                String colNm = dds.DS_ENG_NAME
                subjectTemplate = mongoTemplatePool.getByName(MongoDBConfigConstants.SUB_DATA_SOURCE)
                subjectTemplate.dropCollection(colNm + "_tmp")
                // 创建时保存索引
                MongoUtils.addIndex(subjectTemplate, colNm + "_tmp", dsIndexLst);
                //查询数据并保存到mongodb
//                rawSQLService.queryRawSql(StringUtils.ClobToString(dds.SQL_CLAUSE), new SubjectResultHandler(subjectTemplate, props, colNm + "_tmp"))
                ResultHandler resultHandler = new SubjectResultHandler(subjectTemplate, props, colNm + "_tmp",mailService);
                executeHandler(dds.db_name,resultHandler,StringUtils.ClobToString(dds.SQL_CLAUSE),null,dds.DS_ENG_NAME);

                if (subjectTemplate.collectionExists(colNm + "_tmp")) {
                    subjectTemplate.getCollection(colNm + "_tmp").rename(colNm, true)
                }
            } else if (dds.DS_TYP == 3) {   //静态分段
                /**
                 * 静态分段：数据源名称作为集合名称，分段数据保存到统一集合，在分段字段添加索引加快查询效率，
                 * 这样处理能有效解决因为分段创建集合过多导致mongo服务停止的问题
                 *
                 * nijiang
                 */
                String colNm = dds.DS_ENG_NAME
                subjectTemplate = mongoTemplatePool.getByName(MongoDBConfigConstants.SUB_DATA_SOURCE)
                // 获取更新参数的sql
                ParamObj[] params = new ObjectMapper().readValue(dds.PARAM_DFT_VAL, ParamObj[].class)
                // 按照参数名称字母排序组成mongoDB集合名称
                Collections.sort(Arrays.asList(params));
                // 设置各参数类型
                Map<String, Long> paramMap = new HashMap<String, Long>();
                // 数据源加入参数的SQL
                params.each {
                    // 获取各自参数的类型
                    def list = xmlRawSQLService.queryRawSqlByKey("selectParamType", [param_code: it.type]);
                    if (list != null && list.size() > 0) {
                        paramMap.put(it.name, list.get(0).param_typ);
                    }
                }
                //替换更新条件sql参数格式
                String updParamsql = StringUtils.ClobToString(dds.DS_PARAM);

                updParamsql = replaceSql(updParamsql, [new ParamObj(name: "BGN_TIME"), new ParamObj(name: "END_TIME")] as ParamObj[])
                String sql = replaceSql(StringUtils.ClobToString(dds.SQL_CLAUSE), params);
                //查询分段数据
                Map<String, Object> sqlParam = new HashMap<String, Object>();
                sqlParam.put("BGN_TIME", DateUtils.date2String(tmMap.beginDateTime, DateUtils.FORMAT_DATETIME))
                sqlParam.put("END_TIME", DateUtils.date2String(tmMap.endDateTime, DateUtils.FORMAT_DATETIME))

//                List<Map<String, Object>> piecewiseList = rawSQLService.queryRawSql(updParamsql, sqlParam);
                List<Map<String, Object>> piecewiseList = executeSqlByDbName(dds.db_name,updParamsql, sqlParam,dds.DS_ENG_NAME)
                for (Map<String, Object> obj : piecewiseList) {
                    //设置参数值
                    Map<String, Object> sqlParams = new HashMap<String, Object>();
                    String idxVal = ""
                    paramMap.each { key, value ->
                        if (value == 10) {
                            sqlParams.put(key, Long.valueOf(obj.get(key)+""))
                        } else if (value == 20) {
                            sqlParams.put(key, obj.get(key)+"")
                        } else if (value == 30) {
                            sqlParams.put(key, obj.get(key))
                        }
                    }
                    for(int i=0;i<params.length;i++){
                        if (paramMap.get(params[i].name) == 30) {
                            String paramDate = DateUtils.date2String(obj.get(params[i].name), DateUtils.FORMAT_DATE);
                            idxVal += "_" + paramDate
                        } else if(paramMap.get(params[i].name) == 10) {
                            idxVal += "_" + Long.valueOf(obj.get(params[i].name)+"")
                        }else {
                            idxVal += "_" + obj.get(params[i].name)
                        }
                    }
                    idxVal = idxVal.replaceFirst("_", "");
                    //删除原有数据临时集合
                    subjectTemplate.dropCollection(colNm+"-"+idxVal+ "-tmp");
                    //创建索引
                    MongoUtils.addIndex(subjectTemplate, (colNm+"-"+idxVal) + "-tmp", dsIndexLst);
                    //保存数据
//                    rawSQLService.queryRawSql(sql, sqlParams, new SubjectResultHandler(subjectTemplate, props, colNm+"-"+idxVal+"-tmp"))
                    ResultHandler resultHandler =  new SubjectResultHandler(subjectTemplate, props, colNm+"-"+idxVal+"-tmp",mailService);
                    executeHandler(dds.db_name,resultHandler,sql,sqlParams,dds.DS_ENG_NAME);
                    //将临时集合改名为正式集合
                    if (subjectTemplate.collectionExists((colNm+"-"+idxVal) + "-tmp")) {
                        subjectTemplate.getCollection((colNm+"-"+idxVal) + "-tmp").rename(colNm+"-"+idxVal, true)
                    }
                }
            } else if (dds.DS_TYP == 7) {     //增量数据源初始化
                // 获取更新参数的sql
                ParamObj[] params = new ObjectMapper().readValue(StringUtils.ClobToString(dds.UPD_KEY), ParamObj[].class)
                // 按照参数名称字母排序组成mongoDB集合名称
                Collections.sort(Arrays.asList(params));
                String colNm = dds.DS_ENG_NAME
                subjectTemplate = mongoTemplatePool.getByName(MongoDBConfigConstants.SUB_DATA_SOURCE)

                // 设置各参数类型
                Map<String, Long> paramMap = new HashMap<String, Long>();
                //全部的参数名称列表
                List<String> paramNames = new ArrayList<String>();
                // 数据源加入参数的SQL
                params.each {
                    paramNames.add(it.name)
                    // 获取各自参数的类型
                    def list = xmlRawSQLService.queryRawSqlByKey("selectParamType", [param_code: it.type]);
                    if (list != null && list.size() > 0) {
                        paramMap.put(it.name, list.get(0).param_typ);
                    }
                }
                //替换更新数据sql参数格式
                String updSql = StringUtils.ClobToString(dds.SQL_CLAUSE);
                updSql = replaceSql(updSql, params)
                //sql参数设置
                Map<String, Object> sqlParam = new HashMap<String, Object>();
                sqlParam.put("BGN_TIME", DateUtils.date2String(tmMap.beginDateTime, DateUtils.FORMAT_DATETIME))
                sqlParam.put("END_TIME", DateUtils.date2String(tmMap.endDateTime, DateUtils.FORMAT_DATETIME))
                //替换删除条件sql参数格式
                String delsql = StringUtils.ClobToString(dds.DEL_PARAM);
                // 不存在删除参数时，不执行删除操作
                if (delsql != null && delsql != "") {
                    // 同步数据删除(必须在增量更新之前)
                    delsql = replaceSql(delsql, [new ParamObj(name: "BGN_TIME"), new ParamObj(name: "END_TIME")] as ParamObj[])
//                    rawSQLService.queryRawSql(delsql, sqlParam,
//                            new SubjectIncParamsResultHandler(subjectTemplate, rawSQLService, colNm, updSql, paramMap, paramNames, props, MongoDBConfigConstants.DEL_TYPE))
                    ResultHandler resultHandler = new SubjectIncParamsResultHandler(subjectTemplate, rawSQLService, colNm, updSql, paramMap, paramNames, props, MongoDBConfigConstants.DEL_TYPE);
                    executeHandler(dds.db_name,resultHandler,delsql,sqlParam,dds.DS_ENG_NAME);
                }
                //替换更新条件sql参数格式
                String updParamsql = StringUtils.ClobToString(dds.UPD_PARAM);
                updParamsql = replaceSql(updParamsql, [new ParamObj(name: "BGN_TIME"), new ParamObj(name: "END_TIME")] as ParamObj[])

                //更新数据
//                rawSQLService.queryRawSql(updParamsql, sqlParam,
//                        new SubjectIncParamsResultHandler(subjectTemplate, rawSQLService, colNm, updSql, paramMap, paramNames, props, MongoDBConfigConstants.UPD_TYPE))
                ResultHandler resultHandler =  new SubjectIncParamsResultHandler(subjectTemplate, rawSQLService, colNm, updSql, paramMap, paramNames, props, MongoDBConfigConstants.UPD_TYPE);
                executeHandler(dds.db_name,resultHandler,updParamsql,sqlParam,dds.DS_ENG_NAME);
            }
            subjectStatusService.updateDsStatus(dds, TxtAndRrpStatusService.DS_STAS_NORMAL, tmMap.endDateTime)
        } catch (err) {
            subjectStatusService.updateDsStatus(dds, TxtAndRrpStatusService.DS_STAS_ERROR, tmMap.endDateTime)
            logger.error("数据源[" + dds.DS_ENG_NAME + "]增量更新失败：" + err)
        }
        def end = System.currentTimeMillis()
        logger.info("数据源[" + dds.DS_ENG_NAME + "]增量更新结束,共耗时：" + (end - start) + "ms");
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
            logger.error("数据源【"+ds_name+"】获取数据连接失败，将使用默认连接尝试获取数据,错误信息："+e);
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
            logger.error("数据源【"+ds_name+"】执行sql错误：---"+err);
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
            logger.error("数据源【"+ds_name+"】获取数据连接失败，将使用默认连接尝试获取数据,错误信息："+e);
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
            logger.error("数据源【"+ds_name+"】执行sql错误：---"+err);
        }
        return null
    }

    /**
     * 追加新的专题数据源
     */
    void appendNewSubToMgDB() {
        //获取所有静态及分段数据源
        List<Map<String, Object>> dataSrcs = xmlRawSQLService.queryRawSqlByKey("selectAllDataSrc")
        dataSrcs.each() {
            def subStatus = subjectStatusService.getSubStatusByDsName(it.DS_ENG_NAME)
            if (!it.DS_ENG_NAME.equals("ENTERPRISEBASICDATA_1") && subStatus == null) {
                Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                    public void uncaughtException(Thread t, Throwable e) {
                        logger.error("追加专题数据源【"+it.DS_ENG_NAME + "】 错误信息:" + e.toString());
                    }
                });
                producerPool.execute(new ThreadPoolTask(it,true,"save"))
//                saveSubObj(it, false)
            }
        }
    }

    /**
     * 数据源字段属性变更
     * @param dateTimeMap
     */
    void updateDsProChg(Map<String, String> dateTimeMap) {
        List<Map<String, Object>> dataSrcs = xmlRawSQLService.queryRawSqlByKey("selectDsProChgByDate", dateTimeMap)
        for (Map<String, String> dds : dateTimeMap) {
            if ("ENTERPRISEBASICDATA_1".equals(dds.DS_ENG_NAME)) {
                continue;
            }
            //删除数据源
            mongoTemplatePool.dropDbBase(dds.DS_ENG_NAME)
            SubjectStatus subjectStatus = subjectStatusService.getSubStatusByDsName(dds.DS_ENG_NAME)
            //只更新已存在的数据源数据
            if (subjectStatus != null) {
                if (subjectStatus.DS_STAS == TxtAndRrpStatusService.DS_STAS_UPDATE) {
                    continue;
                }
                saveSubObj(dds, true);
            }
        }
    }

    /**
     * 获取需要扫描更新的专题数据源信息
     * @param colNm
     * @param dateTimeMap
     * @param tblTimeMap
     * @param dsTimeMap
     * @return
     */
    List<Map<String, Object>> getChgTbl(String colNm, Map<String, String> dateTimeMap,
                                        Map<String, Map<String, Object>> tblTimeMap,
                                        Map<String, HashMap<String, Date>> dsTimeMap) {
        List<Map<String, Object>> tbls = xmlRawSQLService.queryRawSqlByKey("selectAllTbl")
        MongoTemplate col = mongoTemplatePool.getByName(colNm)
        //保存数据源名称
        List<String> needTbls = new ArrayList<String>();
        //保存需要更新的数据源信息
        List<Map<String, Object>> dsLst = new ArrayList<Map<String, Object>>();
        //去除重复的数据源名称
        List<String> ddsNms = new ArrayList<String>();
        tbls.each {
            if (!needTbls.contains(it.TBL_NAME)) {
                needTbls.add(it.TBL_NAME);
                try {
                    String sql = 'select max(Upd_Time) UPD_TIME from ' + it.TBL_NAME
                    List<Map<String, Object>> result = rawSQLService.queryRawSql(sql)
                    Date maxTblDate = null;
                    if (result != null && result.size() > 0) {
                        maxTblDate = result.get(0).UPD_TIME
                        maxTblDate = DateUtils.string2Date(DateUtils.date2String(maxTblDate, DateUtils.FORMAT_DATETIME), DateUtils.FORMAT_DATETIME);
                    }else{
                        maxTblDate = new Date();
                    }
                    Date beginDate = DateUtils.string2Date(dateTimeMap.beginDateTime, DateUtils.FORMAT_DATETIME);
                    Map<String, Object> lastObj = col.findOne(new Query(Criteria.where("TBL_NAME").is(it.TBL_NAME)), Map.class,colNm)
                    Date lastTblUpdTime;
                    boolean noFlg = false;
                    // 保存各表的最新更新时间
                    if (null == lastObj) {
                        noFlg = true;
                        lastTblUpdTime = beginDate;
                        tblTimeMap.put(it.TBL_NAME, ["TBL_NAME": it.TBL_NAME, "UPD_TIME": maxTblDate]);
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
                            Date lastUpdTimeStr = null;
                            if (dsStatus != null) {
                                lastUpdTimeStr = dsStatus.LAST_UPD_TIME
                            }
                            // 源的上次更新时间为空则使用表的上次更新时间
                            if (null == lastUpdTimeStr) {
                                lastUpdTimeStr = lastTblUpdTime;
                            }

                            // 源的更新时间为空则使用表的更新填充
                            if (dsTimeMap.get(dsNm) == null) {
                                dsTimeMap.put(dsNm, ["beginDateTime": lastUpdTimeStr, "endDateTime": maxTblDate]);
                            }
                            // 给每个源填入更新开始时间和更新结束时间
                            // 开始扫描时间
                            if (dsTimeMap.get(dsNm).get("beginDateTime").compareTo(lastTblUpdTime) > 0) {
                                dsTimeMap.get(dsNm).put("beginDateTime", lastTblUpdTime);
                            }
                            // 结束扫描时间
                            if (dsTimeMap.get(dsNm).get("endDateTime").compareTo(maxTblDate) < 0) {
                                dsTimeMap.get(dsNm).put("endDateTime", maxTblDate);
                            }
                            // 加入到数据源数组
                            if (!ddsNms.contains(dsNm)) {
                                ddsNms.add(dsNm);
                                dsLst.add(dds);
                            }
                        }
                    }
                } catch (err) {
                    logger.error("=============="+err)
                }
            }
        }
        return dsLst
    }

    /**
     * 保存每个表的更新时间
     * @param tblTimeMap
     * @param colName
     */
     void saveTblTimeMapToMg(HashMap<String, Object> tblTimeMap, String colName) {
        MongoOperations  tblTimesTemplate = mongoTemplatePool.getByName(colName)
        tblTimeMap.each { key,val->
            tblTimesTemplate.upsert(new Query(Criteria.where("TBL_NAME").is(key)), val, colName);
        }
    }
    /**
     * 删除不存在的数据源
     */
    void delNoUseDs() {
        // 系统级源名称
        List<String> constDsNm = new ArrayList<String>();
        constDsNm.add(MongoDBConfigConstants.ADMIN);
        constDsNm.add(MongoDBConfigConstants.LOCAL);
        constDsNm.add(MongoDBConfigConstants.INDEX_ALL_DB);
        constDsNm.add(MongoDBConfigConstants.TXT_BLT_DB);
        constDsNm.add(MongoDBConfigConstants.TXT_NWS_DB);
        constDsNm.add(MongoDBConfigConstants.TXT_WCJ_DB);
        constDsNm.add(MongoDBConfigConstants.TXT_YCNC_DB);
        constDsNm.add(MongoDBConfigConstants.TXT_LAW_DB);
        constDsNm.add(MongoDBConfigConstants.TXT_TIP_DB);
        constDsNm.add(MongoDBConfigConstants.RRP_BAS_DB);
        constDsNm.add(MongoDBConfigConstants.DS_STATUS_DB);
        constDsNm.add(MongoDBConfigConstants.IDX_STATUS_DB);
        constDsNm.add(MongoDBConfigConstants.TXT_RRP_STATUS_DB);
        constDsNm.add(MongoDBConfigConstants.SUB_TBL_UPD_TIME);
        constDsNm.add(MongoDBConfigConstants.IDX_TBL_UPD_TIME);
        constDsNm.add(MongoDBConfigConstants.IDX_MARCO);
        constDsNm.add(MongoDBConfigConstants.IDX_MARCO_UPD);
        constDsNm.add(MongoDBConfigConstants.SUB_DATA_SOURCE);
        //获取所有静态及分段源名称
        List<Map<String, Object>> dataSrcs = xmlRawSQLService.queryRawSqlByKey("selectAllDataSrcNm")
        //获取mongo全部的数据源
        List<String> dbBaseNames = mongoTemplatePool.getDbBaseNames()
        //获取DS_STATUS表中的数据
        List<SubjectStatus> dsStatsList = subjectStatusService.findAll()
        //删除无效的数据源
        dbBaseNames.each {
            if (!dataSrcs.contains([ds_eng_name: it]) && !constDsNm.contains(it) && !it.contains("ZZBB_")
                    && !it.contains("HQ_")) {
                mongoTemplatePool.dropDbBase(it)
                subjectStatusService.removeDsStatus(it)
            }
        }
        //删除状态表中不存在的数据源数据
        dsStatsList.each {
            if (!dataSrcs.contains([ds_eng_name: it.DS_NAME])) {
                subjectStatusService.removeDsStatus(it.DS_NAME)
            }
        }
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

    /**
     * 获取最近一次扫描时间
     * @return
     */
    Map<String, String> getLastScanTime() {
        String lastScanTimeStr = PropertiesUtil.getProperty(PropertiesUtil.LAST_SCAN_TIME_FILE,"sub.lastScanTime");
        Date date = new Date();
        String beginDateTime = null;
        String endDateTime = null;
        if (lastScanTimeStr == null || lastScanTimeStr.equals("")) {
            endDateTime = DateUtils.date2String(date, DateUtils.FORMAT_DATETIME);
            beginDateTime = DateUtils.date2String(DateUtils.addMinutes(date, -10), DateUtils.FORMAT_DATETIME);
        } else {
            endDateTime = DateUtils.date2String(date, DateUtils.FORMAT_DATETIME);
            beginDateTime = lastScanTimeStr;
        }
        return ["beginDateTime": beginDateTime, "endDateTime": endDateTime]
    }

    /**
     *  多线程处理
     *  */
    private class ThreadPoolTask implements Runnable, Serializable {
        def Map<String,Object> dds=null;
        def boolean flg = false;
        def String type="";
        def Map<String, Date> tmMap;
        //保存
        public ThreadPoolTask(Map<String,Object> dds,boolean flg,String type){
            this.dds = dds;
            this.flg = flg;
            this.type=type;
        }
        //修改
        public ThreadPoolTask(Map<String,Object> dds,String type,Map<String, Date> tmMap){
            this.dds = dds;
            this.type=type;
            this.tmMap = tmMap;
        }

        @Override
        public void run() {
            try {
                if(type=="save"){
                    saveSubObj(dds, flg)
                }else if(type=="update"){
                    updateSubObj(dds,tmMap);
                }
                Thread.sleep(1);
            } catch (Exception e) {
                if(type=="save"){
                    logger.error("数据源["+dds.DS_ENG_NAME+"]初始化失败，错误信息："+e)
                }else if(type=="update"){
                    logger.error("数据源["+dds.DS_ENG_NAME+"]增量更新失败，错误信息："+e)
                }
                throw new RuntimeException(e);
            }
        }
    }
}
