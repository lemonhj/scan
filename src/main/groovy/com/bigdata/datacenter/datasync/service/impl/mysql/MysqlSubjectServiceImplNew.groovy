package com.bigdata.datacenter.datasync.service.impl.mysql

import com.avaje.ebean.EbeanServer
import com.avaje.ebean.SqlQuery
import com.avaje.ebean.SqlRow
import com.bigdata.datacenter.datasync.enums.DsTypeEnum
import com.bigdata.datacenter.datasync.enums.ScanStateEnum
import com.bigdata.datacenter.datasync.enums.ScanTrigEnum
import com.bigdata.datacenter.datasync.enums.ScanTypeEnum
import com.bigdata.datacenter.datasync.model.mongodb.ParamObj
import com.bigdata.datacenter.datasync.model.mongodb.SubjectStatus
import com.bigdata.datacenter.datasync.quartz.MysqlSubjectServiceJobNew
import com.bigdata.datacenter.datasync.quartz.SubjectServiceJobNew
import com.bigdata.datacenter.datasync.server.mysql.MysqlSubjectServer
import com.bigdata.datacenter.datasync.service.*
import com.bigdata.datacenter.datasync.service.impl.MailService
import com.bigdata.datacenter.datasync.service.impl.SubjectEsIncUpdParamsResultHandler
import com.bigdata.datacenter.datasync.utils.*
import com.bigdata.datacenter.datasync.utils.constants.MongoDBConfigConstants
import com.bigdata.datacenter.metadata.data.EbeanServerPool
import com.bigdata.datacenter.metadata.service.RawSQLService
import com.bigdata.datacenter.metadata.service.ResultHandler
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.log4j.Logger
import org.quartz.*
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

import javax.annotation.Resource
import java.sql.Clob
import java.sql.SQLException
import java.text.ParseException
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.function.Consumer

/**
 * mysql版---专题数据源
 * Created by haiyang on 2017/11/20.
 */
//@Service(value = "mysql-subject-new")
class MysqlSubjectServiceImplNew implements ScanService {

    private static final Logger logger = Logger.getLogger(MysqlSubjectServiceImplNew.class)
    @Autowired
    MysqlSubjectServiceImplNew subjectServiceImplNew
    @Autowired
    Scheduler schedulerMysql
    @Autowired
    RawSQLService rawSQLService
    @Autowired
    XmlRawSQLService xmlRawSQLService
//    @Resource(name = "mysql-subjectStatusService")
    SubjectStatusService subjectStatusService
//    @Autowired
//    MongoTemplatePool mongoTemplatePool
//    MongoOperations subjectTemplate;
    @Autowired
    DBCommon_mysql subjectTemplate
    @Autowired
    EbeanServerPool ebeanServerPool;
    @Autowired
    EbeanServer ebeanServer
    @Autowired
    ScanMonitorService scanMonitorService

    public static List<String> dsNms = null;
    // 周期类型的job
    public static final String PRID = "PRID";
    // 手动立即触发的job
    public static final String IMID = "IMID";

    ThreadPoolExecutor producerPool = null;

    //保存错误数据源信息，用于邮件发送和错误数据源的整理
    static Map<String, Map<String, String>> errorMap = null;

    //邮件服务
    @Autowired
    MailService mailService

    //job当前数量
    static jobNum = 0;
    //job最大数量
    static maxActive = 5;

    static {
//        producerPool = new ThreadPoolExecutor(10, 10, 5, TimeUnit.MINUTES, new LinkedBlockingQueue<Runnable>(),
//                new ThreadPoolExecutor.CallerRunsPolicy());
        String maxActi = PropertiesUtil.getProperty("quartz.max.active.num")
        if (maxActi != null && maxActi != "") {
            try {
                maxActive = Integer.parseInt(maxActi);
            } catch (e) {
                maxActive = 5
            }
        }
        errorMap = new Hashtable<String, Map<String, String>>();
    }

    void resetDsStats() {
        //重置数据源状态，将状态1全部改成状态0
        logger.debug("服务启动，数据源状态表中数据源的状态为1的数据全部重置为0")
        subjectStatusService.resetDsStatus(1, 0);
    }

    /**
     * 增量更新
     */
    @Override
    void incrementalSync() {
        //添加扫描监控数据
        scanMonitorService.addScanTrig(ScanTrigEnum.UPDATE)
        logger.info("专题数据源增量更新开始")
        long startTime = System.currentTimeMillis();
        //获取更新时间
        logger.debug("增量更新，获取最近一次扫描时间")
        Map<String, String> dateTimeMap = getLastScanTime();
        try {
            if (jobNum > maxActive) {
                return
            }
            jobNum++
            producerPool = new ThreadPoolExecutor(10, 15, 30, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(),
                    new ThreadPoolExecutor.CallerRunsPolicy());

            //追加新的专题数据源
            logger.debug("追加新的数据源【appendNewSubToMgDB()】开始")
            appendNewSubToMgDB()
            logger.debug("追加新的数据源【appendNewSubToMgDB()】结束")

            // 修改数据源属性变更
            logger.debug("判断数据源属性变更是否需要重新扫描数据源")
            if (PropertiesUtil.getProperty("sub.prochg").equals("true")) {
                logger.debug("执行属性变更需要重新扫描数据源的逻辑开始")
                updateDsProChg(dateTimeMap);
                logger.debug("执行属性变更需要重新扫描数据源的逻辑结束")
            }
            // 删除不存在的数据源
            try {
                //TODO 删除不存在的数据源暂时不开发
//                logger.debug("删除不存在的数据源【delNoUseDs()】开始")
//                delNoUseDs();
//                logger.debug("删除不存在的数据源【delNoUseDs()】结束")
            } catch (e) {
//                logger.error("[专题数据源增量更新]删除无效数据失败,错误信息："+e)
                boolean errFlg = saveErrMsg("subjectdatasource", e.getMessage(), "delete")
                if (errFlg) {
                    mailService.sendMail("专题数据源增量更新删除无效数据源失败", "【专题数据源】增量更新删除无效数据源失败，错误信息：" + e)
                }
            }

            //获取需要更新的数据源信息
            Map<String, Map<String, Date>> dsTimeMap = new HashMap<String, Map<String, Date>>(); //t
            Map<String, Map<String, Object>> tblTimeMap = new HashMap<String, Map<String, Object>>();
            logger.debug("获取需要更新的数据源列表数据【getChgTbl()】开始")
            List<Map<String, Object>> dataSrcs = getChgTbl(MongoDBConfigConstants.SUB_TBL_UPD_TIME, dateTimeMap, tblTimeMap, dsTimeMap);
//            logger.debug("获取需要更新的数据源列表数据【getChgTbl()】结束，需要更新的数据源为：" + dataSrcs)
            for (Map<String, Object> dds : dataSrcs) {
                if (dds.DS_ENG_NAME.equals("ENTERPRISEBASICDATA_1")) {
                    continue;
                }
                //处理指定数据源
                if (dsNms != null && dsNms.size() > 0) {
                    logger.debug("指定的数据源为:" + dsNms.toArray())
                    if (!dsNms.contains(dds.DS_ENG_NAME)) {
                        continue
                    }
                }
                //添加扫描监控记录
                println("-->>need scan ds:" + dds.OBJ_ID)
                logger.info("-->>need scan ds:" + dds.OBJ_ID)
                scanMonitorService.updateScanDsCheckState(dds.OBJ_ID + "", true)
                //记录数据源数据到扫描监控表
                scanMonitorService.addScanDs(dds.DS_ENG_NAME, dds.OBJ_ID + "", ScanTypeEnum.UPDATE)

                Map<String, Date> tmMap = dsTimeMap.get(dds.DS_ENG_NAME);
                logger.debug("增量更新的数据源【" + dds.DS_ENG_NAME + "】更新时间：" + tmMap)

                producerPool.execute(new ThreadPoolTask(dds, "update", tmMap, TrigUtils.getCurrentTrigId()))
//                producerPool.execute(new ThreadPoolTask(dds, "update", tmMap))
//                 updateSubObj(dds, tmMap);
            }
            logger.debug("数据源增量更新完成，修改appliaction.properties文件的sub.lastScanTime的时间为：" + dateTimeMap.endDateTime)
            //记录扫描时间
            PropertiesUtil.writeProperties(PropertiesUtil.LAST_SCAN_TIME_FILE, "sub.lastScanTime", dateTimeMap.endDateTime)

            logger.debug("数据源增量更新完成，修改mongo中数据源本次更新的时间：" + tblTimeMap)
            // 写入每个表的更新时间
            saveTblTimeMapToMg(tblTimeMap, MongoDBConfigConstants.SUB_TBL_UPD_TIME)
            jobNum--;
        } catch (err) {
            jobNum--;
//            logger.error("专题数据源增量更新错误：" + err);
            //记录扫描时间
            PropertiesUtil.writeProperties(PropertiesUtil.LAST_SCAN_TIME_FILE, "sub.lastScanTime", dateTimeMap.beginDateTime)
            boolean errFlg = saveErrMsg("subjectdatasource", err.getMessage(), "update")
            if (errFlg) {
                mailService.sendMail("专题数据源增量更新失败", "【专题数据源】增量更新失败，错误信息：" + err)
            }
        } finally {
//            producerPool.shutdown();
            try {
                if (producerPool != null) {
                    producerPool.shutdown();
                    while (!producerPool.awaitTermination(5, TimeUnit.SECONDS)) {
                    }
                }
                //添加扫描监控数据
            } catch (e) {
//                logger.error("====专题数据源增量更新错误 :" + e);
                boolean errFlg = saveErrMsg("subjectdatasource", e.getMessage(), "update")
                if (errFlg) {
                    mailService.sendMail("专题数据源增量更新失败", "【专题数据源】增量更新失败，错误信息：" + e)
                }
            }
            ESearchHelper.closeClient();
        }
        long endTime = System.currentTimeMillis();
        logger.info("专题数据源增量更新完成,共耗时：" + (endTime - startTime) + "ms");
    }

    /**
     * 全量更新
     */
    @Override
    void totalSync() {
        logger.info("专题数据源初始化开始")
        long startTime = System.currentTimeMillis()
        try {
            producerPool = new ThreadPoolExecutor(10, 10, 30, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(),
                    new ThreadPoolExecutor.CallerRunsPolicy())
            List<Map<String, Object>> dataSrcs = xmlRawSQLService.queryRawSqlByKey("selectAllDataSrc")
            logger.debug("totalSync():获取需要初始化的数据源列表数据开始" + dataSrcs)
            dataSrcs.each() {
                //在进行初始化之前对状态进行修改
                subjectStatusService.updateDsStatus(it, TxtAndRrpStatusService.DS_STAS_NORMAL, null);
                logger.debug("totalSync():将数据源【" + it.DS_ENG_NAME + "】的状态改成0（正常）")
                //判断是否单数据源操作
                if (dsNms != null && dsNms.size() > 0) {
                    if (dsNms.contains(it.DS_ENG_NAME)) {
                        //记录数据源数据到扫描监控表
                        scanMonitorService.addScanDs(it.DS_ENG_NAME, it.OBJ_ID + "", ScanTypeEnum.INIT)
//                        producerPool.execute(new ThreadPoolTask(it, true, "save"))
                        producerPool.execute(new ThreadPoolTask(it, true, "save", TrigUtils.getCurrentTrigId()))
                    }
                } else {
                    //记录扫描时间
                    PropertiesUtil.writeProperties(PropertiesUtil.LAST_SCAN_TIME_FILE, "sub.lastScanTime", DateUtils.date2String(new Date(), DateUtils.FORMAT_DATETIME))
//                    Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
//                        public void uncaughtException(Thread t, Throwable e) {
////                            logger.error("数据源"+it.DS_ENG_NAME + " 初始化错误信息:" + e.toString());
//                            boolean errFlg = saveErrMsg(it.DS_ENG_NAME,e.getMessage(),"save")
//                            if(errFlg) {
//                                mailService.sendMail("专题数据源【" + it.DS_ENG_NAME + "】初始化失败", "专题数据源【" + it.DS_ENG_NAME + "】初始化失败，错误信息：" + e)
//                            }
//                        }
//                    });
                    //记录数据源数据到扫描监控表
                    scanMonitorService.addScanDs(it.DS_ENG_NAME, it.OBJ_ID + "", ScanTypeEnum.INIT)
                    producerPool.execute(new ThreadPoolTask(it, true, "save", TrigUtils.getCurrentTrigId()))
//                    saveSubObj(it, true)
                }
            }
        } catch (err) {
//            logger.error("专题数据源初始化错误：" + err);
            boolean errFlg = saveErrMsg("subjectdatasource", err.getMessage(), "save")
            if (errFlg) {
                mailService.sendMail("专题数据源初始化失败", "【专题数据源】初始化失败，错误信息：" + err)
            }
        } finally {
//            producerPool.shutdown();
            try {
                if (producerPool != null) {
                    producerPool.shutdown();
                    while (!producerPool.awaitTermination(5, TimeUnit.SECONDS)) {
                    }
                }
            } catch (e) {
//                logger.error("专题数据源初始化错误 :" + e.toString());
                boolean errFlg = saveErrMsg("subjectdatasource", e.getMessage(), "save")
                if (errFlg) {
                    mailService.sendMail("专题数据源初始化失败", "【专题数据源】初始化失败，错误信息：" + e)
                }
            }
            ESearchHelper.closeClient();
        }
        long endTime = System.currentTimeMillis();
        logger.info("专题数据初始化完成,共耗时：" + (endTime - startTime) + "ms");
        mailService.sendMail("专题数据源完成初始化扫描", "【专题数据源】初始化完成,共耗时：" + (endTime - startTime) + "ms");
    }

    /**
     * 强制手动刷新数据更新到mysql
     */
    void saveImedSubToShdle() {
        logger.info("强制手动刷新专题数据开始")
        long startTime = System.currentTimeMillis()
        try {
            // 获取sqlMapper
            List<Map<String, Object>> dataSrcs = xmlRawSQLService.queryRawSqlByKey("selectImedDataSrc")
            if (!schedulerMysql.isStarted()) {
                schedulerMysql.start()
            }

            if (dataSrcs != null && dataSrcs.size() > 0) {
                dataSrcs.each {
                    try {
                        chkImdJobTrigger(it, schedulerMysql, it.TRIG_TIME);
                    } catch (Exception e) {
                        // 发送邮件错误日志
//                        logger.error("数据源[" + it.DS_ENG_NAME + "]强制手动刷新失败，错误信息:" + e);
                        boolean errFlg = saveErrMsg(it.DS_ENG_NAME, e.getMessage(), "imed")
                        if (errFlg) {
                            mailService.sendMail("专题数据源【" + it.DS_ENG_NAME + "】强制手动刷新失败", "专题数据源【" + it.DS_ENG_NAME + "】强制手动刷新失败，错误信息：" + e)
                        }
                    }
                }
            }
        } catch (Exception e) {
            // 发送邮件错误日志
//            logger.error("强制手动刷新数据源错误，"+e);
            boolean errFlg = saveErrMsg("subjectdatasource", e.getMessage(), "imed")
            if (errFlg) {
                mailService.sendMail("专题数据源强制手动刷新失败", "【专题数据源】强制手动刷新失败，错误信息：" + e)
            }
        }
        long endTime = System.currentTimeMillis();
        logger.info("强制手动刷新专题数据完成,共耗时：" + (endTime - startTime) + "ms")
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
        String jobName = dataSrcs.DS_ENG_NAME + "Job_Imd"
        // trigger名称
        String triggerName = dataSrcs.DS_ENG_NAME + "Trigger_Imd"
        JobDetail jobDetail = scheduler.getJobDetail(JobKey.jobKey(jobName, IMID))
        Trigger trigger = (Trigger) scheduler.getTrigger(TriggerKey.triggerKey(triggerName, IMID))
        if (jobDetail == null) {
            // 创建任务如果此任务没有在队列中
            jobDetail = JobBuilder.newJob(MysqlSubjectServiceJobNew.class).
                    withIdentity(jobName, IMID).build()
        }
        // trigger为null 或者 trigger开始时间不符时候
        if (trigger == null || (trigger != null && trigger.getStartTime().compareTo(trigTime) != 0)) {
            // 已经存在触发器则删除
            if (trigger != null) {
                scheduler.deleteJob(JobKey.jobKey(jobName, IMID));
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
            jobDetail.getJobDataMap().put("SubjectStatusService", subjectStatusService)
            jobDetail.getJobDataMap().put("subjectServiceImplNew", subjectServiceImplNew)
            jobDetail.getJobDataMap().put("scanMonitorService", scanMonitorService)
            jobDetail.getJobDataMap().put("subjectTemplate", subjectTemplate)
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
            if (!schedulerMysql.isStarted()) {
                schedulerMysql.start();
            }
            if (dataSrcs != null && dataSrcs.size() > 0) {
                dataSrcs.each {
                    try {
                        chkPridJobTrigger(it, schedulerMysql);
                    } catch (Exception e) {
                        // 发送邮件错误日志
//                        logger.error("专题数据源[" + it.DS_ENG_NAME + "]周期刷新失败，错误信息:" + e);
                        boolean errFlg = saveErrMsg(it.DS_ENG_NAME, e.getMessage(), "prid")
                        if (errFlg) {
                            mailService.sendMail("专题数据源【" + it.DS_ENG_NAME + "】周期刷新失败", "专题数据源【" + it.DS_ENG_NAME + "】周期刷新失败，错误信息：" + e)
                        }
                    }
                }
            }
        } catch (Exception e) {
            // 发送邮件错误日志
//            logger.error("周期刷新数据源错误，"+e);
            boolean errFlg = saveErrMsg("subjectdatasource", e.getMessage(), "prid")
            if (errFlg) {
                mailService.sendMail("专题数据源周期刷新失败", "【专题数据源】周期刷新失败，错误信息：" + e)
            }
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
        JobDetail jobDetail = scheduler.getJobDetail(JobKey.jobKey(jobName, PRID));
        String[] triggers = dataSrcs.TIME_RULE.split(";");
        boolean chgFlg = false;
        if (jobDetail == null) {
            chgFlg = true
        } else {
            List<CronTrigger> nowTriggerList = scheduler.getTriggersOfJob(JobKey.jobKey(jobName, PRID))
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
            scheduler.deleteJob(JobKey.jobKey(jobName, PRID));
            // 创建任务如果此任务没有在队列中
            jobDetail = JobBuilder.newJob(SubjectServiceJobNew.class)
                    .withIdentity(jobName, PRID)
                    .storeDurably(true).build();

            // durable, 指明任务就算没有绑定Trigger仍保留在Quartz的JobStore中,
            jobDetail.getJobDataMap().put("DS", dataSrcs);
            jobDetail.getJobDataMap().put("SubjectStatusService", subjectStatusService);
            jobDetail.getJobDataMap().put("subjectServiceImplNew", subjectServiceImplNew);
            // 加入一个任务到Quartz框架中, 等待后面再绑定Trigger,此接口中的JobDetail的durable必须为true
//            scheduler.addJob(jobDetail, false);
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
            logger.debug("saveSubObj(): 获取数据源【" + dds.DS_ENG_NAME + "】状态数据，" +
                    "如果状态为1则直接终止本数据源的初始化操作。---" + status);
            if (status == TxtAndRrpStatusService.DS_STAS_UPDATE) {
//				subjectStatusService.updateDsStatus(dds, TxtAndRrpStatusService.DS_STAS_NORMAL, new Date());
                //更新扫描监控数据
                scanMonitorService.updateScanDsStateByLastDsObjId(dds.OBJ_ID + "", ScanStateEnum.CONFLICT_EXIT)
                return;
            }
            //更新扫描监控数据
            scanMonitorService.updateScanDsStateByLastDsObjId(dds.OBJ_ID + "", ScanStateEnum.SCANING)

            logger.info("专题数据源[" + dds.DS_ENG_NAME + "]数据初始化开始");
            logger.debug("saveSubObj(): 将数据源【" + dds.DS_ENG_NAME + "】的状态修改为: 1(正在更新)");
            //更新状态
            subjectStatusService.updateDsStatus(dds, TxtAndRrpStatusService.DS_STAS_UPDATE, null)
            //获取sql的参数
            List<Map<String, Object>> props = xmlRawSQLService.queryRawSqlByKey("selectDopByObjId", [obj_id: dds.OBJ_ID]);
            logger.debug("saveSubObj(): 获取数据源【" + dds.DS_ENG_NAME + "】的属性列表：---" + props.toString());
            //获取指标属性
            List<Map<String, Object>> dsIndexLst = xmlRawSQLService.queryRawSqlByKey("selectDsIndexByObjId", [obj_id: dds.OBJ_ID]);
            logger.debug("saveSubObj(): 获取数据源【" + dds.DS_ENG_NAME + "】的索引列表：---" + dsIndexLst.toString());

            //由prop信息 建表建索引
            String tableName = dds.DS_ENG_NAME
            subjectTemplate.createTableIndex(tableName, props, dsIndexLst)

            //静态数据源
            if (dds.DS_TYP == 2) {
                String colNm = dds.DS_ENG_NAME
//                subjectTemplate = mongoTemplatePool.getByName(MongoDBConfigConstants.SUB_DATA_SOURCE)
//                logger.debug("saveSubObj(): 删除静态数据源【"+  dds.DS_ENG_NAME + "】的临时集合：---"+colNm + "_tmp");
//                subjectTemplate.dropCollection(colNm + "_tmp")
                // 创建时保存索引
                logger.debug("saveSubObj(): 创建静态数据源【" + dds.DS_ENG_NAME + "】的临时集合，并创建索引。---" + colNm + "_tmp");
//                MongoUtils.addIndex(subjectTemplate, colNm + "_tmp", dsIndexLst);
                //查询数据并保存到mongodb
                logger.debug("saveSubObj(): 执行静态数据源【" + dds.DS_ENG_NAME + "】的SQL，获取数据源数据开始。");
                ResultHandler resultHandler = new MysqlSubjectResultHandler(subjectTemplate, props, colNm, mailService);
//                ResultHandler resultHandler = new MysqlSubjectResultHandler(subjectTemplate, props, colNm + "_tmp", mailService);
                //扫描监控数据-记录数据源记录数
                scanMonitorService.addDsRecordCount(DsTypeEnum.STATIC_DS, dds.OBJ_ID + "", StringUtils.ClobToString(dds.SQL_CLAUSE), null)

                executeHandler(dds.db_name, resultHandler, StringUtils.ClobToString(dds.SQL_CLAUSE), null, dds.DS_ENG_NAME);
                // rawSQLService.queryRawSql(StringUtils.ClobToString(dds.SQL_CLAUSE), new SubjectResultHandler(subjectTemplate, props, colNm + "_tmp"))
                logger.debug("saveSubObj(): 执行静态数据源【" + dds.DS_ENG_NAME + "】的SQL，获取数据源数据结束。");
//                if (subjectTemplate.collectionExists(colNm + "_tmp")) {
//                    subjectTemplate.getCollection(colNm + "_tmp").rename(colNm, true)
//                    logger.debug("saveSubObj(): 将静态数据源【" + dds.DS_ENG_NAME + "】的临时集合改名为正式集合名称");
//                }
            } else if (dds.DS_TYP == 3) {
                /**
                 * 静态分段：数据源名称作为集合名称，分段数据保存到统一集合，在分段字段添加索引加快查询效率，
                 * 这样处理能有效解决因为分段创建集合过多导致mongo服务停止的问题
                 * nijiang
                 */
                String colNm = dds.DS_ENG_NAME
//                subjectTemplate = mongoTemplatePool.getByName(MongoDBConfigConstants.SUB_DATA_SOURCE)

                //TODO 分段的初始化，为什么要删除表？
//                if (subjectTemplate.collectionExists(colNm)) { //判断数据集合是否存在，不存在则创建
//                    logger.debug("saveSubObj(): 判断分段数据源【" + dds.DS_ENG_NAME + "】是否需要删除原集合，true-则删除false则不删除。flag=" + flag);
//                    if (flag) {   //判断是否需要删除原集合
//                        subjectTemplate.dropCollection(colNm)
//                    }
//                } else {
//                    logger.debug("saveSubObj(): 创建分段数据源【" + dds.DS_ENG_NAME + "】的数据源集合");
//                    subjectTemplate.createCollection(colNm)
//                }
                //判断索引列表是否为空，为空则创建
                //TODO 添加索引，由于之前已经添加了，故不需要再次操作了
//                if (dsIndexLst == null || dsIndexLst.size() == 0) {
//                    dsIndexLst = new ArrayList<Map<String, Object>>();
//                }
//                //分段数据源添加为分段字段添加索引
                def idxName = "ext_section_val"
//                dsIndexLst.add([INDEX_NAME: idxName, PROP_NAME: idxName, ORDER_RULE: "desc"])
//                //创建索引
//                logger.debug("saveSubObj(): 创建分段数据源【" + dds.DS_ENG_NAME + "】的索引集合。----" + dsIndexLst);
//                MongoUtils.addIndex(subjectTemplate, colNm, dsIndexLst);
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
                logger.debug("saveSubObj(): 获取数据源【" + dds.DS_ENG_NAME + "】的数据源参数类型列表。----" + paramMap);
                //数据参数格式替换
                String sql = replaceSql(StringUtils.ClobToString(dds.SQL_CLAUSE), params);
                //查询分段数据
//                List<Map<String, Object>> piecewiseList = rawSQLService.queryRawSql(dds.DS_PARAM_FULL);
                List<Map<String, Object>> piecewiseList = executeSqlByDbName(dds.db_name, dds.DS_PARAM_FULL, null, dds.DS_ENG_NAME)
                logger.debug("saveSubObj(): 获取数据源【" + dds.DS_ENG_NAME + "】的分段参数列表。----" + piecewiseList);
                //扫描监控数据-记录数据源记录数
                scanMonitorService.addDsRecordCount(DsTypeEnum.STATIC_DS, dds.OBJ_ID + "", null, piecewiseList.size())

                for (Map<String, Object> obj : piecewiseList) {
                    //设置参数值
                    Map<String, Object> sqlParams = new HashMap<String, Object>();
                    String idxVal = ""
                    paramMap.each { key, value ->
                        if (value == 10) {
                            sqlParams.put(key, Long.valueOf(obj.get(key) + ""))
                        } else if (value == 20) {
                            sqlParams.put(key, obj.get(key) + "")
                        } else if (value == 30) {
                            sqlParams.put(key, obj.get(key))
                        }
                    }
                    for (int i = 0; i < params.length; i++) {
                        if (paramMap.get(params[i].name) == 30) {
                            String paramDate = DateUtils.date2String(obj.get(params[i].name), DateUtils.FORMAT_DATE);
                            idxVal += "_" + paramDate
                        } else if (paramMap.get(params[i].name) == 10) {
                            idxVal += "_" + Long.valueOf(obj.get(params[i].name) + "")
                        } else {
                            idxVal += "_" + obj.get(params[i].name)
                        }
                    }
                    idxVal = idxVal.replaceFirst("_", "");
                    //删除原有数据
                    logger.debug("saveSubObj(): 删除数据源【" + dds.DS_ENG_NAME + "】分段‘" + idxVal + "’的数据");
//                    subjectTemplate.remove(Query.query(Criteria.where(idxName).is(idxVal)), colNm)
                    //TODO 待实现，删除原有数据
//                    subjectTemplate.deleteByParam(colNm,[idxName: idxVal])
                    //保存数据
                    logger.debug("saveSubObj(): 保存数据源【" + dds.DS_ENG_NAME + "】分段‘" + idxVal + "’的数据开始");
                    ResultHandler resultHandler = new MysqlSubjectResultHandler(subjectTemplate, props, colNm, ["idxName": idxName, "idxVal": idxVal], mailService);
                    executeHandler(dds.db_name, resultHandler, sql, sqlParams, dds.DS_ENG_NAME);
                    logger.debug("saveSubObj(): 保存数据源【" + dds.DS_ENG_NAME + "】分段‘" + idxVal + "’的数据结束");
                    // rawSQLService.queryRawSql(sql, sqlParams, new SubjectResultHandler(subjectTemplate, props, colNm, ["idxName": idxName, "idxVal": idxVal]))
                }
            } else if (dds.DS_TYP == 7) {     //增量数据源初始化
                // 获取更新参数的sql
                ParamObj[] params = new ObjectMapper().readValue(StringUtils.ClobToString(dds.UPD_KEY), ParamObj[].class)
                // 按照参数名称字母排序组成mongoDB集合名称
                Collections.sort(Arrays.asList(params));
                String colNm = dds.DS_ENG_NAME
                //TODO 删除临时表
//                subjectTemplate = mongoTemplatePool.getByName(MongoDBConfigConstants.SUB_DATA_SOURCE)
//                logger.debug("saveSubObj(): 删除增量数据源【"+  dds.DS_ENG_NAME + "】的临时集合：---"+colNm + "_tmp");
//                subjectTemplate.dropCollection(colNm + "_tmp")

                // 获取索引不重复属性名称
                List<String> indexFldName = new ArrayList<String>();
                dsIndexLst.each {
                    if (!indexFldName.contains(it.PROP_NAME)) {
                        indexFldName.add(it.PROP_NAME);
                    }
                }

                // 将增量更新参数加入索引队列
                List<String> paramNames = new ArrayList<String>();
                StringBuffer joinSql = new StringBuffer();
                params.each {
                    paramNames.add(it.name)
                    if (!indexFldName.contains(it.name)) {
                        Map<String, Object> idx = [INDEX_NAME: it.name, PROP_NAME: it.name, ORDER_RULE: "asc"]
                        dsIndexLst.add(idx)
                    }
                    //添加SQL_CLAUSE与INIT_SQL的关联关系
                    if (joinSql.toString().length() > 0) {
                        joinSql.append(" AND ");
                    }
                    joinSql.append(" A.").append(it.name).append(" = B.").append(it.name);
                }
                // 创建时保存索引
                logger.debug("saveSubObj(): 添加增量数据源【" + dds.DS_ENG_NAME + "】的索引列表：---" + dsIndexLst)
//                MongoUtils.addIndex(subjectTemplate, colNm + "_tmp", dsIndexLst);
                /**
                 * 增量数据源初始化，通过执行init_sql获取数据源完整的数据结果集
                 * 修改为
                 * 将初始化方式修改成通过init_sql 与 sql_clause关联查询，通过upd_key进行关联，减少sql的执行次数
                 * update 2017-11-13 ni.jiang
                 */
//                ResultHandler resultHandler =  new SubjectIncrementalResultHandler(subjectTemplate, props, colNm + "_tmp", paramNames, MongoDBConfigConstants.INS_TYPE,mailService);
//                executeHandler(dds.db_name,resultHandler,StringUtils.ClobToString(dds.INIT_SQL),null,dds.DS_ENG_NAME);
                StringBuffer sql = new StringBuffer()
                sql.append("SELECT A.* FROM (")
                        .append(StringUtils.ClobToString(dds.SQL_CLAUSE))
                        .append(") A INNER JOIN (")
                        .append(StringUtils.ClobToString(dds.INIT_SQL))
                        .append(") B ON ")
                logger.debug("saveSubObj(): 保存增量数据源【" + dds.DS_ENG_NAME + "】[" + sql.toString() + "] joinSql:[" + joinSql.toString() + "]的数据开始");

                String excuteSql = sql.append(joinSql).toString()
                //扫描监控数据-记录数据源记录数
                scanMonitorService.addDsRecordCount(DsTypeEnum.DYN_DS, dds.OBJ_ID + "", excuteSql, null)

                ResultHandler resultHandler = new MysqlSubjectIncrementalResultHandler(subjectTemplate, props, colNm, paramNames, MongoDBConfigConstants.INS_TYPE, mailService);
//                ResultHandler resultHandler = new SubjectIncrementalResultHandler(subjectTemplate, props, colNm + "_tmp", paramNames, MongoDBConfigConstants.INS_TYPE, mailService);
                executeHandler(dds.db_name, resultHandler, excuteSql, null, dds.DS_ENG_NAME)

                //查询数据并保存到mongodb
                //rawSQLService.queryRawSql(StringUtils.ClobToString(dds.INIT_SQL),
                //      new SubjectIncrementalResultHandler(subjectTemplate, props, colNm + "_tmp", paramNames, MongoDBConfigConstants.INS_TYPE))

//                if (subjectTemplate.collectionExists(colNm + "_tmp")) {
//                    subjectTemplate.getCollection(colNm + "_tmp").rename(colNm, true)
//                    logger.debug("saveSubObj(): 将增量数据源【" + dds.DS_ENG_NAME + "】的临时集合改名为正式集合名称");
//                }
            }
            //更新状态
            subjectStatusService.updateDsStatus(dds, TxtAndRrpStatusService.DS_STAS_NORMAL, new Date())
            //更新扫描监控状态
            scanMonitorService.endScanDs(dds.OBJ_ID + "")
        } catch (err) {
            subjectStatusService.updateDsStatus(dds, TxtAndRrpStatusService.DS_STAS_ERROR, null)
            scanMonitorService.updateScanDsStateByLastDsObjId(dds.OBJ_ID + "", ScanStateEnum.ERR)
//            logger.error("专题数据源[" + dds.DS_ENG_NAME + "]初始化错误：" + err);
            //添加扫描监控错误数据
            scanMonitorService.addErrMsg(dds.OBJ_ID + "", err.getMessage())

            boolean errFlg = saveErrMsg(dds.DS_ENG_NAME, err.getMessage(), "save")
            if (errFlg) {
                mailService.sendMail("专题数据源【" + dds.DS_ENG_NAME + "】初始化失败", "专题数据源【" + dds.DS_ENG_NAME + "】初始化失败，错误信息：" + err)
            }
        }
        def end = System.currentTimeMillis()
        logger.info("专题数据源[" + dds.DS_ENG_NAME + "]数据初始化结束,共耗时：" + (end - start) + "ms");
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
            logger.debug("updateSubObj(): 获取数据源【" + dds.DS_ENG_NAME + "】状态数据，" +
                    "如果状态为1则直接终止本数据源的更新操作。---" + status);
            if (status == TxtAndRrpStatusService.DS_STAS_UPDATE) {
                return;
            }
            //更新扫描监控数据
            scanMonitorService.updateScanDsStateByLastDsObjId(dds.OBJ_ID + "", ScanStateEnum.SCANING)
            logger.info("专题数据源[" + dds.DS_ENG_NAME + "]增量更新开始");
            //更新状态
            logger.debug("updateSubObj(): 将数据源【" + dds.DS_ENG_NAME + "】的状态修改为: 1(正在更新)");
            subjectStatusService.updateDsStatus(dds, TxtAndRrpStatusService.DS_STAS_UPDATE, null)
            //获取sql的参数
            List<Map<String, Object>> props = xmlRawSQLService.queryRawSqlByKey("selectDopByObjId", [obj_id: dds.OBJ_ID]);
            logger.debug("updateSubObj(): 获取数据源【" + dds.DS_ENG_NAME + "】的属性列表：---" + props.toString());
            //获取指标属性
            List<Map<String, Object>> dsIndexLst = xmlRawSQLService.queryRawSqlByKey("selectDsIndexByObjId", [obj_id: dds.OBJ_ID]);
            logger.debug("updateSubObj(): 获取数据源【" + dds.DS_ENG_NAME + "】的索引列表：---" + dsIndexLst.toString());

            //由prop信息 建表建索引
            String tableName = dds.DS_ENG_NAME
            subjectTemplate.createTableIndex(tableName, props, dsIndexLst)

            //静态数据源
            if (dds.DS_TYP == 2) {
                String colNm = dds.DS_ENG_NAME
//                subjectTemplate = mongoTemplatePool.getByName(MongoDBConfigConstants.SUB_DATA_SOURCE)
//                logger.debug("updateSubObj(): 删除静态数据源【" + dds.DS_ENG_NAME + "】的临时集合：---" + colNm + "_tmp");
//                subjectTemplate.dropCollection(colNm + "_tmp")
                // 创建时保存索引
                logger.debug("updateSubObj(): 创建静态数据源【" + dds.DS_ENG_NAME + "】的临时集合，并创建索引。---" + colNm + "_tmp");
//                MongoUtils.addIndex(subjectTemplate, colNm + "_tmp", dsIndexLst);
                //查询数据并保存到mongodb
                logger.debug("updateSubObj(): 执行静态数据源【" + dds.DS_ENG_NAME + "】的SQL，保存数据源数据开始。");
                //扫描监控数据-记录数据源记录数
                scanMonitorService.addDsRecordCount(DsTypeEnum.STATIC_DS, dds.OBJ_ID + "", StringUtils.ClobToString(dds.SQL_CLAUSE), null)
//                rawSQLService.queryRawSql(StringUtils.ClobToString(dds.SQL_CLAUSE), new SubjectResultHandler(subjectTemplate, props, colNm + "_tmp",mailService))
                ResultHandler resultHandler = new MysqlSubjectResultHandler(subjectTemplate, props, colNm, mailService);
//                ResultHandler resultHandler = new SubjectResultHandler(subjectTemplate, props, colNm + "_tmp", mailService);
                executeHandler(dds.db_name, resultHandler, StringUtils.ClobToString(dds.SQL_CLAUSE), null, dds.DS_ENG_NAME);
                logger.debug("updateSubObj(): 执行静态数据源【" + dds.DS_ENG_NAME + "】的SQL，保存数据源数据结束。");
//                if (subjectTemplate.collectionExists(colNm + "_tmp")) {
//                    subjectTemplate.getCollection(colNm + "_tmp").rename(colNm, true)
//                    logger.debug("updateSubObj(): 将静态数据源【" + dds.DS_ENG_NAME + "】的临时集合改名为正式集合名称");
//                }
            } else if (dds.DS_TYP == 3) {   //静态分段
                /**
                 * 静态分段：数据源名称作为集合名称，分段数据保存到统一集合，在分段字段添加索引加快查询效率，
                 * 这样处理能有效解决因为分段创建集合过多导致mongo服务停止的问题
                 *
                 * nijiang
                 */
                String colNm = dds.DS_ENG_NAME
//                subjectTemplate = mongoTemplatePool.getByName(MongoDBConfigConstants.SUB_DATA_SOURCE)
                def idxName = "ext_section_val"
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
                logger.debug("updateSubObj(): 获取数据源【" + dds.DS_ENG_NAME + "】的数据源参数类型列表。----" + paramMap);
                //替换更新条件sql参数格式
                String updParamsql = StringUtils.ClobToString(dds.DS_PARAM);

                updParamsql = replaceSql(updParamsql, [new ParamObj(name: "BGN_TIME"), new ParamObj(name: "END_TIME")] as ParamObj[])
                String sql = replaceSql(StringUtils.ClobToString(dds.SQL_CLAUSE), params);
                //查询分段数据
                Map<String, Object> sqlParam = new HashMap<String, Object>();
                sqlParam.put("BGN_TIME", DateUtils.date2String(tmMap.beginDateTime, DateUtils.FORMAT_DATETIME))
                sqlParam.put("END_TIME", DateUtils.date2String(tmMap.endDateTime, DateUtils.FORMAT_DATETIME))

//                List<Map<String, Object>> piecewiseList = rawSQLService.queryRawSql(updParamsql, sqlParam);
                List<Map<String, Object>> piecewiseList = executeSqlByDbName(dds.db_name, updParamsql, sqlParam, dds.DS_ENG_NAME)
                //扫描监控数据-记录数据源记录数
                scanMonitorService.addDsRecordCount(DsTypeEnum.STATIC_DS, dds.OBJ_ID + "", null, piecewiseList.size())
                logger.debug("updateSubObj(): 获取数据源【" + dds.DS_ENG_NAME + "】的增量更新‘" + sqlParam + "’时间段内分段参数列表。----" + piecewiseList);
                for (Map<String, Object> obj : piecewiseList) {
                    //设置参数值
                    Map<String, Object> sqlParams = new HashMap<String, Object>();
                    String idxVal = ""
                    paramMap.each { key, value ->
                        if (value == 10) {
                            sqlParams.put(key, Long.valueOf(obj.get(key) + ""))
                        } else if (value == 20) {
                            sqlParams.put(key, obj.get(key) + "")
                        } else if (value == 30) {
                            sqlParams.put(key, obj.get(key))
                        }
                    }
                    for (int i = 0; i < params.length; i++) {
                        if (paramMap.get(params[i].name) == 30) {
                            String paramDate = DateUtils.date2String(obj.get(params[i].name), DateUtils.FORMAT_DATE);
                            idxVal += "_" + paramDate
                        } else if (paramMap.get(params[i].name) == 10) {
                            idxVal += "_" + Long.valueOf(obj.get(params[i].name) + "")
                        } else {
                            idxVal += "_" + obj.get(params[i].name)
                        }
                    }

                    idxVal = idxVal.replaceFirst("_", "");
                    //删除原有数据
                    logger.debug("updateSubObj(): 删除数据源【" + dds.DS_ENG_NAME + "】分段‘" + idxVal + "’的数据");
//                    subjectTemplate.remove(Query.query(Criteria.where(idxName).is(idxVal)), colNm)
                    //TODO 删除原有数据
//                    subjectTemplate.remove(Query.query(Criteria.where(idxName).is(idxVal)), colNm)
                    //保存数据
//                    rawSQLService.queryRawSql(sql, sqlParams, new SubjectResultHandler(subjectTemplate, props, colNm, ["idxName": idxName, "idxVal": idxVal],mailService))
                    logger.debug("updateSubObj(): 保存数据源【" + dds.DS_ENG_NAME + "】分段‘" + idxVal + "’[" + sqlParams + "]的数据开始");
                    ResultHandler resultHandler = new MysqlSubjectResultHandler(subjectTemplate, props, colNm, ["idxName": idxName, "idxVal": idxVal], mailService);
                    executeHandler(dds.db_name, resultHandler, sql, sqlParams, dds.DS_ENG_NAME);
                    logger.debug("updateSubObj(): 保存数据源【" + dds.DS_ENG_NAME + "】分段‘" + idxVal + "’的数据结束");
                }
            } else if (dds.DS_TYP == 7) {     //增量数据源初始化
                // 获取更新参数的sql
                ParamObj[] params = new ObjectMapper().readValue(StringUtils.ClobToString(dds.UPD_KEY), ParamObj[].class)
                // 按照参数名称字母排序组成mongoDB集合名称
                Collections.sort(Arrays.asList(params));
                String colNm = dds.DS_ENG_NAME
//                subjectTemplate = mongoTemplatePool.getByName(MongoDBConfigConstants.SUB_DATA_SOURCE)

                // 设置各参数类型
                Map<String, Long> paramMap = new HashMap<String, Long>();
                //全部的参数名称列表
                List<String> paramNames = new ArrayList<String>();
                // 数据源加入参数的SQL
                StringBuffer joinSql = new StringBuffer()
                params.each {
                    paramNames.add(it.name)
                    // 获取各自参数的类型
                    def list = xmlRawSQLService.queryRawSqlByKey("selectParamType", [param_code: it.type]);
                    if (list != null && list.size() > 0) {
                        paramMap.put(it.name, list.get(0).param_typ);
                    }

                    //添加SQL_CLAUSE与INIT_SQL的关联关系
                    if (joinSql.toString().length() > 0) {
                        joinSql.append(" AND ");
                    }
                    joinSql.append(" A.").append(it.name).append(" = B.").append(it.name);
                }
                //扫描监控数据-记录数据源记录数
                scanMonitorService.addDsRecordCount(DsTypeEnum.STATIC_DS, dds.OBJ_ID + "", StringUtils.ClobToString(dds.INIT_SQL), null)
                String updSql = StringUtils.ClobToString(dds.SQL_CLAUSE);
                updSql = replaceSql(updSql, params)
                //sql参数设置
                Map<String, Object> sqlParam = new HashMap<String, Object>();
                sqlParam.put("BGN_TIME", DateUtils.date2String(tmMap.beginDateTime, DateUtils.FORMAT_DATETIME))
                sqlParam.put("END_TIME", DateUtils.date2String(tmMap.endDateTime, DateUtils.FORMAT_DATETIME))
                //替换删除条件sql参数格式
                if (dds.DEL_PARAM != null && dds.DEL_PARAM instanceof Clob) {
                    String delsql = StringUtils.ClobToString(dds.DEL_PARAM);
                    // 不存在删除参数时，不执行删除操作
                    if (delsql != null && delsql != "") {
                        // 同步数据删除(必须在增量更新之前)
                        delsql = replaceSql(delsql, [new ParamObj(name: "BGN_TIME"), new ParamObj(name: "END_TIME")] as ParamObj[])
//                        sqlParam.put("BGN_TIME", "2017-10-18 16:21:17")
//                        sqlParam.put("END_TIME", "2017-11-29 16:21:17")
                        ResultHandler deleteResultHandler = new MysqlSubjectIncrementalResultHandler(subjectTemplate, props, dds.DS_ENG_NAME, paramNames, MongoDBConfigConstants.DEL_TYPE, mailService)
                        executeHandler(dds.db_name, deleteResultHandler, delsql, sqlParam, dds.DS_ENG_NAME)
                    }
                }
                //替换更新条件sql参数格式

                String updParamsql = StringUtils.ClobToString(dds.UPD_PARAM);
                updParamsql = replaceSql(updParamsql, [new ParamObj(name: "BGN_TIME"), new ParamObj(name: "END_TIME")] as ParamObj[])

                //更新前，先执行删除 begin
                ResultHandler deleteResultHandler = new MysqlSubjectIncrementalResultHandler(subjectTemplate, props, dds.DS_ENG_NAME, paramNames, MongoDBConfigConstants.DEL_TYPE, mailService)
                executeHandler(dds.db_name, deleteResultHandler, updParamsql, sqlParam, dds.DS_ENG_NAME)
                //更新前，先执行删除 end

                //更新数据
                /**
                 * 增量数据源增量更新，通过执行UPD_PARAM获取需要更新的参数，将参数替换到SQL_CLAUSE执行获取增量更新的数据
                 * 修改为
                 * 将初始化方式修改成通过UPD_PARAM 与 sql_clause关联查询，通过upd_key进行关联，减少sql的执行次数
                 * update 2017-11-13 ni.jiang
                 */
                StringBuffer dataUpdSql = new StringBuffer();
                dataUpdSql.append("SELECT A.* FROM (")
                        .append(StringUtils.ClobToString(dds.SQL_CLAUSE))
                        .append(") A INNER JOIN (")
                        .append(updParamsql)
                        .append(") B ON ");

                logger.debug("updateSubObj(): 保存增量数据源【" + dds.DS_ENG_NAME + "】[" + sqlParam + "]的数据开始");
                ResultHandler resultHandler = new MysqlSubjectIncrementalResultHandler(subjectTemplate, props, colNm, paramNames, MongoDBConfigConstants.UPD_TYPE, mailService);
                executeHandler(dds.db_name, resultHandler, dataUpdSql.append(joinSql).toString(), sqlParam, dds.DS_ENG_NAME);

                logger.debug("updateSubObj(): 保存增量数据源【" + dds.DS_ENG_NAME + "】[" + sqlParam + "]的数据结束");
            }
            subjectStatusService.updateDsStatus(dds, TxtAndRrpStatusService.DS_STAS_NORMAL, tmMap.endDateTime)
            //更新扫描监控状态
            scanMonitorService.endScanDs(dds.OBJ_ID + "")
        } catch (err) {
            subjectStatusService.updateDsStatus(dds, TxtAndRrpStatusService.DS_STAS_ERROR, null)
//            logger.error("专题数据源[" + dds.DS_ENG_NAME + "]增量更新失败：" + err)
            boolean errFlg = saveErrMsg(dds.DS_ENG_NAME, err.getMessage(), "update")
            if (errFlg) {
                mailService.sendMail("专题数据源【" + dds.DS_ENG_NAME + "】增量更新失败", "专题数据源【" + dds.DS_ENG_NAME + "】增量更新失败，错误信息：" + err)
            }
        }
        def end = System.currentTimeMillis()
        logger.info("专题数据源[" + dds.DS_ENG_NAME + "]增量更新结束,共耗时：" + (end - start) + "ms");
    }

    /***
     * 根据不同的db_name,执行handler
     */
    private void executeHandler(String db_name, ResultHandler resultHandler, String sql, Map params, String ds_name) {
        EbeanServer dbEbeanServer = null;
//        String dbName = db_name == null? "default" : db_name;
//        try{
//            dbEbeanServer = ebeanServerPool.getByName(dbName);
//        }catch (e){
//            dbEbeanServer = ebeanServerPool.getByName("default");
//            boolean errFlg = saveErrMsg(ds_name, e.getMessage())
//            if (errFlg) {
//                logger.error("数据源【" + ds_name + "】获取数据连接失败，将使用默认连接尝试获取数据,错误信息：" + e);
//            }
//        }
        dbEbeanServer = ebeanServer;
        try {
            //判断是否为增量数据源的handler，如果是则需要将数据连接传递到handler里面
            if (resultHandler instanceof MysqlSubjectIncParamsResultHandler) {
                resultHandler.dbEbeanServer = dbEbeanServer
            } else if (resultHandler instanceof SubjectEsIncUpdParamsResultHandler) {
                resultHandler.dbEbeanServer = dbEbeanServer
            }
//            logger.debug("executeHandler(): 保存数据源【"+  ds_name + "】的数据，获取数据源的数据连接。----"+dbName);
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

        } catch (Exception execption) {
            logger.error("数据源【" + ds_name + "】执行sql错误：---" + execption);
            println("数据源【" + ds_name + "】执行sql错误：---" + execption)
            boolean errFlg = saveErrMsg(ds_name, execption.getMessage(), "sql")
            if (errFlg) {
                mailService.sendMail("专题数据源【" + ds_name + "】执行sql错误", "专题数据源【" + ds_name + "】执行sql错误，错误信息：" + execption)
            }
        }
    }

    /***
     * 根据不同的db_name,连接不同的数据库执行sql
     */
    private List<Map<String, Object>> executeSqlByDbName(String db_name, String sql, Map params, String ds_name) {
        EbeanServer dbEbeanServer = null;
//        String dbName = db_name == null? "default" : db_name;
//        try{
//            dbEbeanServer = ebeanServerPool.getByName(dbName);
//        }catch (e){
//            dbEbeanServer = ebeanServerPool.getByName("default");
//            boolean errFlg = saveErrMsg(ds_name, e.getMessage())
//            if (errFlg) {
//                logger.error("数据源【" + ds_name + "】获取数据连接失败，将使用默认连接尝试获取数据,错误信息：" + e);
//            }
//        }
        dbEbeanServer = ebeanServer;
        try {
//            logger.debug("executeSqlByDbName(): 连接【"+  ds_name + "】执行sql。----"+dbName);
            SqlQuery query = dbEbeanServer.createSqlQuery(sql);
            if (params) {
                params.each { key, value ->
                    query.setParameter(key, value)
                }
            }
            return query.findList();
        } catch (err) {
//            logger.error("数据源【"+ds_name+"】执行sql错误：---"+err);
            boolean errFlg = saveErrMsg(ds_name, err.getMessage(), "sql")
            if (errFlg) {
                mailService.sendMail("专题数据源【" + ds_name + "】执行sql错误", "专题数据源【" + ds_name + "】执行sql错误，错误信息：" + err)
            }
        }
        return null
    }

    /**
     * 追加新的专题数据源
     */
    void appendNewSubToMgDB() {
        //获取所有静态及分段数据源
        logger.debug("appendNewSubToMgDB(): 获取全部的数据源列表数据")
        List<Map<String, Object>> dataSrcs = xmlRawSQLService.queryRawSqlByKey("selectAllDataSrc")
        logger.debug("全部的数据源列表数据：----" + dataSrcs)
        dataSrcs.each() {
            //添加到监控检测记录
            scanMonitorService.addScanDsCheck(it.OBJ_ID + "", it.DS_ENG_NAME)

            def subStatus = subjectStatusService.getSubStatusByDsName(it.DS_ENG_NAME)
            if ((subStatus != null && subStatus.LAST_UPD_TIME == null) || (!it.DS_ENG_NAME.equals("ENTERPRISEBASICDATA_1") && subStatus == null)) {
                //扫描监控记录处理
                scanMonitorService.updateScanDsCheckState(it.OBJ_ID + "", true)
                scanMonitorService.addScanDs(it.DS_ENG_NAME, it.OBJ_ID + "", ScanTypeEnum.INIT)
                producerPool.execute(new ThreadPoolTask(it, true, "save", TrigUtils.getCurrentTrigId()))
                logger.debug("appendNewSubToMgDB(): 只有状态表没有该数据源信息的数据源才判断为是需要追加的数据源【" + it.DS_ENG_NAME + "】")
            }
        }
    }

    /**
     * 数据源字段属性变更
     * @param dateTimeMap
     */
    void updateDsProChg(Map<String, String> dateTimeMap) {
        logger.debug("updateDsProChg():获取最近属性有变更的数据源列表数据。")
        List<Map<String, Object>> dataSrcs = xmlRawSQLService.queryRawSqlByKey("selectDsProChgByDate", dateTimeMap)
        logger.debug("updateDsProChg():获取最近属性有变更的数据源列表数据，属性有变动的数据源类别数据为：" + dataSrcs)
        for (Map<String, String> dds : dataSrcs) {
            if ("ENTERPRISEBASICDATA_1".equals(dds.DS_ENG_NAME)) {
                continue;
            }
            //删除数据源
//            mongoTemplatePool.dropDbBase(dds.DS_ENG_NAME)
            SubjectStatus subjectStatus = subjectStatusService.getSubStatusByDsName(dds.DS_ENG_NAME)
            //只更新已存在的数据源数据
            if (subjectStatus != null) {
                if (subjectStatus.DS_STAS == TxtAndRrpStatusService.DS_STAS_UPDATE) {
                    logger.debug("updateDsProChg():正在执行更新操作的数据源不进行处理，不进行删除的数据源集合【" + dds.DS_ENG_NAME + "】")
                    continue;
                }
//                扫描属性变更需要重新初始化此数据源
                logger.debug("updateDsProChg():删除有属性更新的数据源集合【" + dds.DS_ENG_NAME + "】")
//                mongoTemplatePool.getByName(MongoDBConfigConstants.SUB_DATA_SOURCE).dropCollection("dds.DS_ENG_NAME")
                subjectTemplate.dropTable(dds.DS_ENG_NAME)
                logger.debug("updateDsProChg():有属性更新的数据源重新进行初始化【" + dds.DS_ENG_NAME + "】")
//                saveSubObj(dds, true);
                //扫描监控记录处理
                scanMonitorService.updateScanDsCheckState(dds.OBJ_ID + "", true)
                scanMonitorService.addScanDs(dds.DS_ENG_NAME, dds.OBJ_ID + "", ScanTypeEnum.INIT)
                producerPool.execute(new ThreadPoolTask(dds, true, "save", TrigUtils.getCurrentTrigId()))
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
    List<Map<String, Object>> getChgTbl(String colNm,
                                        Map<String, String> dateTimeMap,
                                        Map<String, Map<String, Object>> tblTimeMap,
                                        Map<String, HashMap<String, Date>> dsTimeMap) {
        logger.debug("getChgTbl():获取全部的数据源所涉及到的数据表名列表")
        List<Map<String, Object>> tbls = xmlRawSQLService.queryRawSqlByKey("selectAllTbl")
//        MongoTemplate col = mongoTemplatePool.getByName(colNm)
        //保存数据源名称
        List<String> needTbls = new ArrayList<String>();
        //保存需要更新的数据源信息
        List<Map<String, Object>> dsLst = new ArrayList<Map<String, Object>>();
        //去除重复的数据源名称
        List<String> ddsNms = new ArrayList<String>();
        tbls.each {
            //数据源中心库涉及到的DUAL表过滤掉
            if ("DUAL".equalsIgnoreCase(it.TBL_NAME)) {
                return
            }
            //过滤重复的表名
            if (!needTbls.contains(it.TBL_NAME)) {
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
                        maxTblDate = result.get(0).UPD_TIME
                        maxTblDate = DateUtils.string2Date(DateUtils.date2String(maxTblDate, DateUtils.FORMAT_DATETIME), DateUtils.FORMAT_DATETIME);
                    } else {
                        maxTblDate = new Date();
                    }
                    Date beginDate = DateUtils.string2Date(dateTimeMap.beginDateTime, DateUtils.FORMAT_DATETIME);
//                    Map<String, Object> lastObj = col.findOne(new Query(Criteria.where("TBL_NAME").is(it.TBL_NAME)), Map.class, colNm)
                    Map<String, Object> lastObj = null
                    def rows = subjectTemplate.find(colNm, ["TBL_NAME": it.TBL_NAME])
                    if (rows != null && rows.size() > 0) {
                        lastObj = rows.get(0)
                    }

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
//                  logger.error("=============="+err)
                    saveErrMsg("subjectdatasource", err.getMessage(), "update")
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
        tblTimeMap.each { key, val ->
            subjectTemplate.saveOrUpdate(colName, val, ["TBL_NAME": key])
        }
    }
    /**
     * 删除不存在的数据源
     */
    void delNoUseDs() {
        // 系统级源名称
//        List<String> constDsNm = new ArrayList<String>();
//        constDsNm.add(MongoDBConfigConstants.ADMIN);
//        constDsNm.add(MongoDBConfigConstants.LOCAL);
//        constDsNm.add(MongoDBConfigConstants.INDEX_ALL_DB);
//        constDsNm.add(MongoDBConfigConstants.TXT_BLT_DB);
//        constDsNm.add(MongoDBConfigConstants.TXT_NWS_DB);
//        constDsNm.add(MongoDBConfigConstants.TXT_WCJ_DB);
//        constDsNm.add(MongoDBConfigConstants.TXT_YCNC_DB);
//        constDsNm.add(MongoDBConfigConstants.TXT_LAW_DB);
//        constDsNm.add(MongoDBConfigConstants.TXT_TIP_DB);
//        constDsNm.add(MongoDBConfigConstants.RRP_BAS_DB);
//        constDsNm.add(MongoDBConfigConstants.DS_STATUS_DB);
//        constDsNm.add(MongoDBConfigConstants.IDX_STATUS_DB);
//        constDsNm.add(MongoDBConfigConstants.TXT_RRP_STATUS_DB);
//        constDsNm.add(MongoDBConfigConstants.SUB_TBL_UPD_TIME);
//        constDsNm.add(MongoDBConfigConstants.IDX_TBL_UPD_TIME);
//        constDsNm.add(MongoDBConfigConstants.IDX_MARCO);
//        constDsNm.add(MongoDBConfigConstants.IDX_MARCO_UPD);
//        constDsNm.add(MongoDBConfigConstants.SUB_DATA_SOURCE);

        //获取所有静态及分段源名称
        logger.debug("delNoUseDs(): 查询数据库中全部的数据源列表数据")
        List<Map<String, Object>> dataSrcs = xmlRawSQLService.queryRawSqlByKey("selectAllDataSrcNm")
        logger.debug("全部的数据源列表数据：------" + dataSrcs)
        //获取mongo全部的数据源
        logger.debug("delNoUseDs(): 查询mongo里面全部的数据源名称列表")
//        Set<String> dbBaseNames = mongoTemplatePool.getByName(MongoDBConfigConstants.SUB_DATA_SOURCE).getCollectionNames()
        logger.debug("mongo里面全部的数据源名称列表：------" + dbBaseNames)
        //获取DS_STATUS表中的数据
        logger.debug("delNoUseDs(): 查询mongo里面数据源状态表里面全部数据源数据列表")
        List<SubjectStatus> dsStatsList = subjectStatusService.findAll()
        logger.debug("mongo里面数据源状态表里面全部数据源数据列表：---" + dsStatsList)
        //删除无效的数据源
//        dbBaseNames.each {
//            if (!dataSrcs.contains([ds_eng_name: it]) && !it.contains("ZZBB_") && !it.contains("HQ_")) {
//                logger.debug("delNoUseDs(): 删除无效的数据源【" + it + "】")
//                mongoTemplatePool.getByName(MongoDBConfigConstants.SUB_DATA_SOURCE).dropCollection(it)
//            }
//        }
//        dbBaseNames.each {
//            if (!dataSrcs.contains([ds_eng_name: it]) && !constDsNm.contains(it) && !it.contains("ZZBB_")
//                    && !it.contains("HQ_")) {
//                mongoTemplatePool.dropDbBase(it)
//                subjectStatusService.removeDsStatus(it)
//            }
//        }
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
        String lastScanTimeStr = PropertiesUtil.getProperty(PropertiesUtil.LAST_SCAN_TIME_FILE, "sub.lastScanTime");
        Date date = new Date();
        String beginDateTime = null;
        String endDateTime = null;
        logger.debug("getLastScanTime():判断application.properties表里面是否有‘sub.lastScanTime’的最新时间")
        if (lastScanTimeStr == null || lastScanTimeStr.equals("")) {
            endDateTime = DateUtils.date2String(date, DateUtils.FORMAT_DATETIME);
            //beginDateTime = DateUtils.date2String(DateUtils.addMinutes(date, -10), DateUtils.FORMAT_DATETIME);  TODO li
            //beginDateTime = DateUtils.date2String(DateUtils.addMinutes(date, -10000), DateUtils.FORMAT_DATETIME);
            logger.debug("getLastScanTime():根据当前服务器时间减10分钟作为本次扫描的开始时间")
            beginDateTime = DateUtils.date2String(DateUtils.addMinutes(date, -10), DateUtils.FORMAT_DATETIME);
        } else {
            logger.debug("getLastScanTime():获取application.properties表里面‘sub.lastScanTime’的最新时间作为本次的开始时间")
            endDateTime = DateUtils.date2String(date, DateUtils.FORMAT_DATETIME);
            beginDateTime = lastScanTimeStr;
        }
        logger.debug("getLastScanTime():本次扫描的时间为：" + ["beginDateTime": beginDateTime, "endDateTime": endDateTime]);
        return ["beginDateTime": beginDateTime, "endDateTime": endDateTime]
    }

    /**
     *  多线程处理
     *  */
    private class ThreadPoolTask implements Runnable, Serializable {

        def Map<String, Object> dds = null;
        def boolean flg = false;
        def String type = "";
        def Map<String, Date> tmMap;
        String currentTrigId = null
        //保存
        public ThreadPoolTask(Map<String, Object> dds, boolean flg, String type) {
            this.dds = dds;
            this.flg = flg;
            this.type = type;
        }
        //保存,并记录TRIG_ID变量
        public ThreadPoolTask(Map<String, Object> dds, boolean flg, String type, String currentTrigId) {
            this.currentTrigId = currentTrigId
            this.dds = dds;
            this.flg = flg;
            this.type = type;
        }
        //修改
        public ThreadPoolTask(Map<String, Object> dds, String type, Map<String, Date> tmMap) {
            this.dds = dds;
            this.type = type;
            this.tmMap = tmMap;
        }

        //修改,并记录TRIG_ID变量
        public ThreadPoolTask(Map<String, Object> dds, String type, Map<String, Date> tmMap, String currentTrigId) {
            this.currentTrigId = currentTrigId
            this.dds = dds;
            this.type = type;
            this.tmMap = tmMap;
        }

        @Override
        public void run() {
            try {
                //绑定线程变量
                TrigThreadContext.bindCurrentTrigId(currentTrigId)
                if (type == "save") {
                    logger.debug("数据源【" + dds.DS_ENG_NAME + "】初始化开始")
                    //初始化之前进行状态表修改
                    subjectStatusService.removeDsStatus(dds.DS_ENG_NAME);
                    saveSubObj(dds, flg)
                    logger.debug("数据源【" + dds.DS_ENG_NAME + "】初始化完成")
                } else if (type == "update") {
                    logger.debug("数据源【" + dds.DS_ENG_NAME + "】增量更新开始")
                    updateSubObj(dds, tmMap);
                    logger.debug("数据源【" + dds.DS_ENG_NAME + "】增量更新完成")
                }
            } catch (Exception e) {
                if (type == "save") {
//                    logger.error("数据源["+dds.DS_ENG_NAME+"]初始化失败，错误信息："+e)
                    boolean errFlg = saveErrMsg(dds.DS_ENG_NAME, e.getMessage(), "save")
                    if (errFlg) {
                        mailService.sendMail("专题数据源【" + dds.DS_ENG_NAME + "】初始化失败", "数据源[" + dds.DS_ENG_NAME + "]初始化失败，错误信息：" + e)
                    }
                } else if (type == "update") {
//                    logger.error("数据源["+dds.DS_ENG_NAME+"]增量更新失败，错误信息："+e)
                    boolean errFlg = saveErrMsg(dds.DS_ENG_NAME, e.getMessage(), "update")
                    if (errFlg) {
                        mailService.sendMail("专题数据源【" + dds.DS_ENG_NAME + "】增量更新失败", "数据源[" + dds.DS_ENG_NAME + "]增量更新失败，错误信息：" + e)
                    }
                }
                subjectStatusService.updateDsStatus(dds, TxtAndRrpStatusService.DS_STAS_ERROR, null);
//                throw new RuntimeException(e);
            }
        }
    }

    //保存错误日志，并发送邮件
    static boolean saveErrMsg(String dsName, String errmsg, String type) {
        try {
            Map<String, String> dsErrMsgs = null;
            if (errorMap.containsKey(dsName)) {
                dsErrMsgs = errorMap.get(dsName);
            } else {
                dsErrMsgs = new HashMap<String, String>();
            }
            String key = ""
            if (errmsg != null && errmsg != "") {
                key = errmsg.split("\n")[0];
            } else {
                key = errmsg + ""
            }

            String msgKey = Md5.md5(key);
            /**
             * 判断错误信息是否在errormap里面是否存在，如果不存在则发送邮件，并打印错误信息并记录错误日志的输出时间
             * 如果存在，则判断上次错误信息的时间是否为前一天的记录，如果是则输出错误日志，
             * 同一数据源同一错误日志一天只输出一次，同一错误邮件共只发送一次
             */
            if (!dsErrMsgs.containsKey(msgKey)) {
                dsErrMsgs.put(msgKey, errmsg);
                dsErrMsgs.put("lastDate", DateUtils.date2String(new Date(), DateUtils.FORMAT_DATE));
                writeErrolog(dsName, type, errmsg);
                errorMap.put(dsName, dsErrMsgs);
                return true;
            } else {
                Date lastDate = DateUtils.string2Date(dsErrMsgs.get("lastDate"), DateUtils.FORMAT_DATE)
                Date nowDate = DateUtils.string2Date(DateUtils.date2String(new Date(), DateUtils.FORMAT_DATE), DateUtils.FORMAT_DATE)
                if (lastDate < nowDate) {
                    dsErrMsgs.put("lastDate", DateUtils.date2String(new Date(), DateUtils.FORMAT_DATE));
                    writeErrolog(dsName, type, errmsg);
                }
            }
        } catch (err) {
            saveErrMsg("error", err.getMessage(), "error")
        }
        return false
    }

    static void writeErrolog(String dsName, String type, String errmsg) {
        if (dsName == "subjectdatasource") {
            if (type == "save") {
                logger.error("专题数据源初始化错误 :" + errmsg);
            } else if (type == "update") {
                logger.error("专题数据源增量更新错误：" + errmsg);
            } else if (type == "delete") {
                logger.error("[专题数据源增量更新]删除无效数据失败,错误信息：" + errmsg)
            } else if (type == "imed") {
                logger.error("强制手动刷新数据源错误，" + errmsg);
            } else if (type == "prid") {
                logger.error("周期刷新数据源错误，" + errmsg);
            } else if (type == "append") {
                logger.error("追加新专题数据源错误信息:" + errmsg);
            }
        } else if (dsName == "error") {
            logger.error("==============错误信息:" + errmsg);
        } else {
            if (type == "save") {
                logger.error("数据源【" + dsName + "】初始化失败，错误信息：" + errmsg)
            } else if (type == "update") {
                logger.error("数据源【" + dsName + "】增量更新失败，错误信息：" + errmsg)
            } else if (type == "append") {
                logger.error("追加专题数据源【" + dsName + "】 错误信息:" + errmsg);
            } else if (type == "sql") {
                logger.error("数据源【" + dsName + "】执行sql错误：---" + errmsg)
            } else if (type == "imed") {
                logger.error("数据源【" + dsName + "】强制手动刷新失败，错误信息:" + errmsg);
            } else if (type == "prid") {
                logger.error("数据源【" + dsName + "】周期刷新失败，错误信息:" + errmsg);
            } else if (type == "data") {
                logger.error("数据源【" + dsName + "】保存数据源错误，错误信息:" + errmsg)
            }
        }
    }
}
