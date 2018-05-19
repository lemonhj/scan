package com.bigdata.datacenter.datasync.service.impl

import com.avaje.ebean.SqlRow
import com.bigdata.datacenter.datasync.model.mongodb.Law
import com.bigdata.datacenter.datasync.model.mongodb.LawResult
import com.bigdata.datacenter.metadata.service.ResultHandler
import com.bigdata.datacenter.datasync.utils.EntityUtil
import org.apache.log4j.Logger
import org.springframework.data.mongodb.core.MongoOperations
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.Update

/**
 * 提示信息数据回调并保存到mongo数据库
 * Created by qq on 2017/5/24.
 */
class TxtLawResultHandler implements ResultHandler{
    private static final Logger logger = Logger.getLogger(TxtLawResultHandler.class)
    def Class clazz;
    def MongoOperations txtLawTemplate;
    def MailService mailService;

    TxtLawResultHandler(){}

    TxtLawResultHandler(MongoOperations txtLawTemplate, Class clazz){
        this.txtLawTemplate = txtLawTemplate;
        this.clazz = clazz;
    }

    TxtLawResultHandler(MongoOperations txtLawTemplate, Class clazz,MailService mailService){
        this.txtLawTemplate = txtLawTemplate;
        this.clazz = clazz;
        this.mailService = mailService;
    }
    @Override
    void execute(SqlRow row){
        logger.debug("数据入库开始");
        def start = System.currentTimeMillis()
        try{
            //Map转换为对象
            def lawRes = EntityUtil.mapToObject(row,LawResult.class)
            // 删除废除的法律条文
            if (lawRes.IS_VALID == '0') {
                def result = txtLawTemplate.findOne(new Query(Criteria.where("ID").is(lawRes.ID)),clazz)
                if(result!=null){
                    txtLawTemplate.remove(result)
                }
            }else{
                if (lawRes.CONT != null) {
                    def law = Law.setValue(lawRes,clazz.newInstance())
                    if(law.COM_ID.size()==0){
                        law.COM_ID = null
                    }
                    if(law.INDU_ID.size()==0){
                        law.INDU_ID = null
                    }
                    def result = txtLawTemplate.findOne(new Query(Criteria.where("ID").is(lawRes.ID)),clazz)
                    if (result == null) {
                        txtLawTemplate.save(law);
                    } else {
                        law.idx = result.idx
                        Update up = EntityUtil.objectToUpdate(law)
                        txtLawTemplate.updateFirst(new Query(Criteria.where("ID").is(law.ID)), up, clazz);
                    }
                }
            }
        }catch (err){
            logger.error("法律法规数据保存失败: "+err+",数据为：" + row )
            if(mailService!=null){
                mailService.sendMail("法律法规保存数据失败","【法律法规】保存数据失败,错误信息："+err);
            }
        }
        def end = System.currentTimeMillis()
        logger.debug("数据入库完成，耗时："+(end-start))
    }
}
