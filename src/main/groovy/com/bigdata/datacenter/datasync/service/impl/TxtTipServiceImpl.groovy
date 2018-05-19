package com.bigdata.datacenter.datasync.service.impl

import com.bigdata.datacenter.datasync.core.data.MongoTemplatePool
import com.bigdata.datacenter.datasync.service.ScanService
import com.bigdata.datacenter.datasync.service.TxtAndRrpStatusService
import com.bigdata.datacenter.datasync.service.XmlRawSQLService
import com.bigdata.datacenter.datasync.utils.DateUtils
import com.bigdata.datacenter.datasync.utils.constants.MongoDBConfigConstants
import com.bigdata.datacenter.datasync.model.mongodb.Tip
import com.bigdata.datacenter.datasync.model.mongodb.TipTmp
import com.bigdata.datacenter.datasync.model.mongodb.TipPros
import com.bigdata.datacenter.metadata.service.RawSQLService
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.data.domain.Sort
import org.springframework.data.mongodb.core.MongoOperations
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.stereotype.Service

/**
 * 提示信息
 * Created by qq on 2017/5/19.
 */
@Service(value = "txt-tip")
class TxtTipServiceImpl implements ScanService{
    private static final Logger logger = Logger.getLogger(TxtTipServiceImpl.class)
    @Autowired
    RawSQLService rawSQLService
    @Autowired
    XmlRawSQLService xmlRawSQLService
    @Autowired
    TxtAndRrpStatusService txtAndRrpStatusService
    @Autowired
    MongoTemplatePool mongoTemplatePool
    MongoOperations txtTipTemplate;

    //邮件服务
    @Autowired
    MailService mailService
    /**
     * 增量更新
     */
    @Override
    void incrementalSync() {
        logger.info("提示信息增量更新开始")
        long startTime = System.currentTimeMillis();
        try{
            txtTipTemplate = mongoTemplatePool.getByName(MongoDBConfigConstants.TXT_TIP_DB)
            //提示信息状态修改为正在更新
            txtAndRrpStatusService.updateDsStatus(MongoDBConfigConstants.TXT_TIP_DB,TxtAndRrpStatusService.DS_STAS_UPDATE)
            //获取最新数据时间
            Query query=new Query();
            query.with(new Sort(new Sort.Order(Sort.Direction.DESC,"UPD_TIME")));
            Tip tip = txtTipTemplate.findOne(query,Tip.class)
            if(tip!=null){
                Date maxUpDate = tip.UPD_TIME
                xmlRawSQLService.queryRawSqlByKey("selectTipsByMaxUpdTime",[maxUpDate : maxUpDate],new TxtTipResultHandler(txtTipTemplate,Tip.class,mailService))
            }
            //提示信息状态修改为正常，表示更新完成
            txtAndRrpStatusService.updateDsStatus(MongoDBConfigConstants.TXT_TIP_DB,TxtAndRrpStatusService.DS_STAS_NORMAL)
        }catch (err){
            logger.error("提示信息增量更新错误："+ err );
            txtAndRrpStatusService.updateDsStatus(MongoDBConfigConstants.TXT_TIP_DB, TxtAndRrpStatusService.DS_STAS_ERROR)
             mailService.sendMail("提示信息增量更新失败","【提示信息】增量更新失败,错误信息："+err);

        }
        long endTime = System.currentTimeMillis();
        logger.info("提示信息增量更新完成,共耗时："+(endTime-startTime)+"ms");
    }

    /**
     * 全量更新
     */
    @Override
    void totalSync() {
          logger.info("提示信息初始化开始")
          long startTime = System.currentTimeMillis();
          try{
               txtTipTemplate = mongoTemplatePool.getByName(MongoDBConfigConstants.TXT_TIP_DB)
               //提示信息状态修改为正在更新
               txtAndRrpStatusService.updateDsStatus(MongoDBConfigConstants.TXT_TIP_DB,TxtAndRrpStatusService.DS_STAS_UPDATE)
              //判断提示信息临时表是否存在，存在则删除
              if(txtTipTemplate.collectionExists(MongoDBConfigConstants.TXT_TIP_DB+"_tmp")){
                  txtTipTemplate.dropCollection(MongoDBConfigConstants.TXT_TIP_DB+"_tmp")
              }
              //查询数据并保存到mongodb
              xmlRawSQLService.queryRawSqlByKey("selectAllTip",new TxtTipResultHandler(txtTipTemplate,TipTmp.class,mailService))
               //将临时表更名为正常表
               txtTipTemplate.getCollection(MongoDBConfigConstants.TXT_TIP_DB+"_tmp").rename(MongoDBConfigConstants.TXT_TIP_DB,true)
               //保存属性类型
               savePros();
               //提示信息状态修改为正常，表示更新完成
               txtAndRrpStatusService.updateDsStatus(MongoDBConfigConstants.TXT_TIP_DB,TxtAndRrpStatusService.DS_STAS_NORMAL)
          }catch (err){
                logger.error("提示信息初始化错误："+ err );
                txtAndRrpStatusService.updateDsStatus(MongoDBConfigConstants.TXT_TIP_DB, TxtAndRrpStatusService.DS_STAS_ERROR)
                mailService.sendMail("提示信息初始化失败","【提示信息】初始化失败,错误信息："+err);
          }
           long endTime = System.currentTimeMillis();
           logger.info("提示信息初始化完成,共耗时："+(endTime-startTime)+"ms");
            mailService.sendMail("提示信息完成初始化扫描","【提示信息】初始化完成,共耗时："+(endTime-startTime)+"ms");
    }

    /**
     * 删除6个月之前的数据
     */
    void delData(){
        logger.info("删除提示信息数据开始")
        long start = System.currentTimeMillis()
        try{
            Date endDateBfSix = DateUtils.addMonths(new Date(), -6);
            txtTipTemplate.remove(new Query(Criteria.where("END_DT").lte(endDateBfSix) ),MongoDBConfigConstants.TXT_TIP_DB)
        }catch (err){
            logger.error("删除提示信息失败："+err)
        }
        long end = System.currentTimeMillis()
        logger.info("删除提示信息数据完成，耗时："+(end-start)+"ms")
    }
    /**
     * 保存提示信息属性
     */
    void savePros(){
        List<TipPros> list = new ArrayList<TipPros>()
        list.add(new TipPros("ID","Decimal"))
        list.add(new TipPros("SECU_ID","Decimal"))
        list.add(new TipPros("PUB_DT","DateTime"))
        list.add(new TipPros("BGN_DT","DateTime"))
        list.add(new TipPros("END_DT","DateTime"))
        list.add(new TipPros("NTC_CONT","String"))
        list.add(new TipPros("TYP_CODEI","Decimal"))
        list.add(new TipPros("TYP_CODEII","Decimal"))
        list.add(new TipPros("TYP_NAMEI","String"))
        list.add(new TipPros("TYP_NAMEII","String"))
        list.add(new TipPros("ENT_TIME","DateTime"))
        list.add(new TipPros("UPD_TIME","DateTime"))
        list.add(new TipPros("RS_ID","String"))
        list.add(new TipPros("TRD_CODE","String"))
        list.add(new TipPros("SECU_SHT","String"))
        list.add(new TipPros("BSI_TYP_CODEI","Decimal"))
        list.add(new TipPros("LST_STS_CODE","Decimal"))
        list.add(new TipPros("TYP_SUPER","IntArray"))

        try{
            //判断属性集合是否存在，存在则删除
            if(txtTipTemplate.collectionExists(TipPros.class)){
                txtTipTemplate.dropCollection(TipPros.class);
            }
            //批量保存属性值
            txtTipTemplate.insertAll(list);
            logger.info("提示信息属性创建完成！")
        }catch (err){
            logger.error("提示信息属性创建错误："+err)
        }
    }
}
