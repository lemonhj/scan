package com.bigdata.datacenter.datasync.elasticjob.mysql

import com.bigdata.datacenter.datasync.service.impl.mysql.MysqlSubjectServiceImplNew
import com.dangdang.ddframe.job.api.ShardingContext
import com.dangdang.ddframe.job.api.simple.SimpleJob
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier

/**
 * CRON扫描JOB
 * Created by haiyangp on 2018/05/02.
 */
class CronUpdatePridSubjectJob implements SimpleJob {
    private Logger logger = Logger.getLogger(CronUpdatePridSubjectJob.class)

    @Autowired
    @Qualifier(value = "subjectServiceNew")
    private MysqlSubjectServiceImplNew scanService

    @Override
    void execute(ShardingContext shardingContext) {
        logger.info("CronUpdatePridSubjectJob execute")
        scanService.savePridSubToShdle()
    }
}