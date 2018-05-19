package com.bigdata.datacenter.datasync.elasticjob.mysql

import com.bigdata.datacenter.datasync.service.ScanService
import com.dangdang.ddframe.job.api.ShardingContext
import com.dangdang.ddframe.job.api.simple.SimpleJob
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier

/**
 * 核心定时扫描JOB
 * Created by haiyangp on 2018/05/02.
 */
class UpdateSubjectJob implements SimpleJob {
    private Logger logger =Logger.getLogger(UpdateSubjectJob.class)
    @Autowired
    @Qualifier(value = "subjectServiceNew")
    private ScanService scanService

    @Override
    void execute(ShardingContext shardingContext) {
        logger.info("UpdateSubjectJob execute.Time:" + new Date().toString())
        scanService.incrementalSync()
    }
}