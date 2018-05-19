package com.bigdata.datacenter.datasync.quartz

import com.bigdata.datacenter.datasync.enums.ScanStateEnum
import com.bigdata.datacenter.datasync.enums.ScanTypeEnum
import com.bigdata.datacenter.datasync.model.mongodb.SubjectStatus
import com.bigdata.datacenter.datasync.service.ScanMonitorService
import com.bigdata.datacenter.datasync.service.SubjectStatusService
import com.bigdata.datacenter.datasync.service.TxtAndRrpStatusService
import com.bigdata.datacenter.datasync.service.impl.SubjectServiceImpl
import com.bigdata.datacenter.datasync.service.impl.mysql.DBCommon_mysql
import com.bigdata.datacenter.datasync.service.impl.mysql.MysqlSubjectServiceImplNew
import com.bigdata.datacenter.datasync.utils.DateUtils
import org.apache.log4j.Logger
import org.quartz.Job
import org.quartz.JobDataMap
import org.quartz.JobExecutionContext
import org.quartz.JobExecutionException

/**
 * 专题Job服务
 * Created by haiyangp on 2017/11/27.
 */
class MysqlSubjectServiceJobNew implements Job {
    private static final Logger logger = Logger.getLogger(MysqlSubjectServiceJobNew.class)

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        try {
            JobDataMap jobDataMap = context.getMergedJobDataMap();
            Map<String, Object> dds = (Map<String, Object>) jobDataMap.get("DS");
            MysqlSubjectServiceImplNew subjectService = (MysqlSubjectServiceImplNew) jobDataMap.get("subjectServiceImplNew")
            SubjectStatusService subjectStatusService = (SubjectStatusService) jobDataMap.get("SubjectStatusService")
            DBCommon_mysql subjectTemplate = (DBCommon_mysql) jobDataMap.get("subjectTemplate")
            ScanMonitorService scanMonitorService = (ScanMonitorService) jobDataMap.get("scanMonitorService")
            if (dds == null) {
                throw new Exception("DS is not exist in jobDataMap.");
            }
            if (subjectService == null) {
                throw new Exception("SubjectServiceImpl is not exist in jobDataMap.");
            }
            if (subjectStatusService == null) {
                throw new Exception("SubjectStatusService is not exist in jobDataMap.");
            }
            // 写回存储过程
            String group = context.getJobDetail().key.group
            if (group.equals(SubjectServiceImpl.IMID)) {
                //刷新之前进行状态表修改
//                subjectStatusService.removeDsStatus(dds.DS_ENG_NAME);
                def status = subjectStatusService.getDsStatus(dds.DS_ENG_NAME)
                if (status == TxtAndRrpStatusService.DS_STAS_UPDATE) {
                    logger.error("JOB执行失败，数据源正在进行扫描")
                    return
                }
                //刷新前先删除此表
                subjectTemplate.dropTable(dds.DS_ENG_NAME)
                //添加监控数据
                scanMonitorService.updateScanDsCheckState(dds.OBJ_ID + "", true)
                scanMonitorService.addScanDs(dds.DS_ENG_NAME, dds.OBJ_ID + "", ScanTypeEnum.INIT)
                // 手动刷新
                subjectService.saveSubObj(dds, true);
                subjectService.rawSQLService.callProcedure("{call proc_DSK_DATA_SOUR_SUP(?)}", dds.ID)
                // subjectService.callProcUpd(dds.ID);
            } else {
                // 按照时间规则刷新
                HashMap<String, Date> tmMap = new HashMap<String, Date>();
                SubjectStatus obj = subjectStatusService.getSubStatusByDsName(dds.DS_ENG_NAME)
                if (obj == null) {
                    tmMap.put("beginDateTime", DateUtils.string2Date("1990-01-01", DateUtils.FORMAT_DATE));
                } else {
                    Date lastUpdTime = (Date) obj.LAST_UPD_TIME;
                    if (lastUpdTime == null) {
                        tmMap.put("beginDateTime", DateUtils.string2Date("1990-01-01", DateUtils.FORMAT_DATE));
                    } else {
                        tmMap.put("beginDateTime", lastUpdTime);
                    }
                }
                tmMap.put("endDateTime", new Date());
                // 时间规则更新
                subjectService.updateSubObj(dds, tmMap);
            }
        } catch (Exception e) {
            logger.error(e.toString());
        }
    }
}
