package com.bigdata.datacenter.datasync.service.impl

import com.bigdata.datacenter.datasync.core.data.MongoTemplatePool
import com.bigdata.datacenter.datasync.service.SubjectStatusService
import com.bigdata.datacenter.datasync.utils.constants.MongoDBConfigConstants
import com.bigdata.datacenter.datasync.model.mongodb.Subject
import com.bigdata.datacenter.datasync.model.mongodb.SubjectStatus
import com.bigdata.datacenter.datasync.model.mongodb.TxtRrpStatus
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.data.mongodb.core.MongoOperations
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.Update
import org.springframework.stereotype.Service

import static org.springframework.data.mongodb.core.query.Criteria.where
import static org.springframework.data.mongodb.core.query.Query.query

/**
 * Created by Dean on 2017/5/22.
 */
@Service
class SubjectStatusServiceImpl implements SubjectStatusService{
    @Autowired
    MongoTemplatePool mongoTemplatePool

    /**
     * 获取全部的数据源状态信息
     * @return
     */
    List<SubjectStatus> findAll(){
        MongoOperations mongoOps = mongoTemplatePool.getByName(MongoDBConfigConstants.DS_STATUS_DB)
        return mongoOps.findAll(SubjectStatus.class)
    }

    /**
     * 根据数据源名称查询数据源信息
     * @param dsNm
     * @return
     */
    SubjectStatus getSubStatusByDsName(String dsNm){
        MongoOperations mongoOps = mongoTemplatePool.getByName(MongoDBConfigConstants.DS_STATUS_DB)
        return mongoOps.findOne(query(where("DS_NAME").is(dsNm)), SubjectStatus.class)
    }
    /**
     * 获取数据源状态
     * @param dsNm
     * @return
     */
     int getDsStatus(String dsNm) {
         int status = -1;
         MongoOperations mongoOps = mongoTemplatePool.getByName(MongoDBConfigConstants.DS_STATUS_DB)
         SubjectStatus subject = mongoOps.findOne(query(where("DS_NAME").is(dsNm)), SubjectStatus.class)
         if (null != subject) {
             status = subject.DS_STAS;
         }
        return status;
    }

    /**
     * 更新数据源状态
     * @param dds
     * @param status
     * @param lastUpdTime
     */
    @Override
    void updateDsStatus(Map<String,Object> dds, int status,Date lastUpdTime) {
        MongoOperations mongoOps = mongoTemplatePool.getByName(MongoDBConfigConstants.DS_STATUS_DB)
        SubjectStatus subject = mongoOps.findOne(query(where("DS_NAME").is(dds.DS_ENG_NAME)), SubjectStatus.class)

        if (subject == null) {
            subject = new SubjectStatus()
            subject.with {
                DS_ID = dds.OBJ_ID
                DS_NAME = dds.DS_ENG_NAME
                DS_UPD_TIME = new Date()
                DS_CRT_TIME = new Date()
                DS_STAS = status
            }
            mongoOps.save(subject)
        } else {
            Update update =new Update()
            update.set("DS_STAS", status)
            update.set("DS_UPD_TIME", new Date())
            if(status ==2){
                update.set("ERR_RUN_NUM", !subject.ERR_RUN_NUM ?  1 : subject.ERR_RUN_NUM + 1);
            }else if(status ==0){
                update.set("ERR_RUN_NUM", null);
            }
            if(lastUpdTime!=null){
                update.set("LAST_UPD_TIME", lastUpdTime)
            }
            mongoOps.updateFirst(query(where("DS_NAME").is(dds.DS_ENG_NAME)), update, SubjectStatus.class)
        }
    }

    /**
     * 重置数据源状态
     * @param dds
     * @param status
     * @param lastUpdTime
     */
    @Override
    void resetDsStatus(int oldStatus,int status) {
        MongoOperations mongoOps = mongoTemplatePool.getByName(MongoDBConfigConstants.DS_STATUS_DB)
        //将错误次数清空
        mongoOps.updateMulti(query(where("ERR_RUN_NUM").gt(0)),new Update().set("ERR_RUN_NUM",null), MongoDBConfigConstants.DS_STATUS_DB)
        //将状态数据为1的数据，改成0
        mongoOps.updateMulti(query(where("DS_STAS").is(oldStatus)), new Update().set("DS_STAS", status), MongoDBConfigConstants.DS_STATUS_DB)
    }

    /**
     * 删除数据源状态
     * @param dsName
     */
    @Override
    void removeDsStatus(String dsName){
        MongoOperations mongoOps = mongoTemplatePool.getByName(MongoDBConfigConstants.DS_STATUS_DB)
        mongoOps.remove(new Query(Criteria.where("DS_NAME").is(dsName)),MongoDBConfigConstants.DS_STATUS_DB)
    }
}
