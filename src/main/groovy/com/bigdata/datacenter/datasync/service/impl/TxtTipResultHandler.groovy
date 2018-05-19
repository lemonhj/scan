package com.bigdata.datacenter.datasync.service.impl

import com.avaje.ebean.SqlRow
import com.bigdata.datacenter.datasync.utils.DateUtils
import com.bigdata.datacenter.datasync.model.mongodb.TipSupTyp
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
class TxtTipResultHandler implements ResultHandler{
    private static final Logger logger = Logger.getLogger(TxtTipResultHandler.class)
    def Class clazz;
    def MongoOperations txtTipTemplate;
    def MailService mailService
    TxtTipResultHandler(){}

    TxtTipResultHandler(MongoOperations txtTipTemplate,Class clazz){
        this.txtTipTemplate = txtTipTemplate;
        this.clazz = clazz;
    }

    TxtTipResultHandler(MongoOperations txtTipTemplate,Class clazz,MailService mailService){
        this.txtTipTemplate = txtTipTemplate;
        this.clazz = clazz;
        this.mailService = mailService;
    }

    @Override
    void execute(SqlRow row){
        try{
            //Map转换为对象
            def tipTmp = EntityUtil.mapToObject(row,clazz)

            def tip = txtTipTemplate.findOne(new Query(Criteria.where("ID").is(tipTmp.ID)),clazz);
            if (tipTmp.TYP_CODEI != null) {
                List<Integer> superTypeLst = TipSupTyp.getSupTypCdLst(tipTmp.TYP_CODEI);
                tipTmp.setTYP_SUPER(superTypeLst);
            }
            if (tipTmp.END_DT == null) {
                if (tipTmp.TYP_CODEI.equals(new Double(18))) {
                    tipTmp.setEND_DT(DateUtils.string2Date("9999-12-30", DateUtils.FORMAT_DATE));
                }
            }
            if (tip == null) {
                txtTipTemplate.save(tipTmp)
            } else {
                tipTmp.setIdx(tip.idx);
                Update up = EntityUtil.objectToUpdate(tipTmp)
                txtTipTemplate.updateFirst(new Query(Criteria.where("ID").is(tipTmp.ID)),up,clazz)
            }
        }catch (err){
            logger.error("提示信息数据保存错误："+err)
            if(mailService!=null){
                 mailService.sendMail("提示信息保存数据失败","【提示信息】保存数据失败,错误信息："+err);
            }
        }
    }
}
