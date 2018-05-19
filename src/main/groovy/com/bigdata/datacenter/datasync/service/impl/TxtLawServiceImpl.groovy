package com.bigdata.datacenter.datasync.service.impl

import com.bigdata.datacenter.datasync.core.data.MongoTemplatePool
import com.bigdata.datacenter.datasync.service.ScanService
import com.bigdata.datacenter.datasync.service.TxtAndRrpStatusService
import com.bigdata.datacenter.datasync.service.XmlRawSQLService
import com.bigdata.datacenter.datasync.utils.constants.MongoDBConfigConstants
import com.bigdata.datacenter.datasync.model.mongodb.Law
import com.bigdata.datacenter.datasync.model.mongodb.LawPros
import com.bigdata.datacenter.datasync.model.mongodb.LawTmp
import com.bigdata.datacenter.metadata.service.RawSQLService
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.data.domain.Sort
import org.springframework.data.mongodb.core.MongoOperations
import org.springframework.data.mongodb.core.query.Query
import org.springframework.stereotype.Service

/**
 * 法律法规
 * Created by qq on 2017/5/19.
 */
@Service(value = "txt-law")
class TxtLawServiceImpl implements ScanService{
    private static final Logger logger = Logger.getLogger(TxtLawServiceImpl.class)
    @Autowired
    RawSQLService rawSQLService
    @Autowired
    XmlRawSQLService xmlRawSQLService
    @Autowired
    TxtAndRrpStatusService txtAndRrpStatusService
    @Autowired
    MongoTemplatePool mongoTemplatePool
    MongoOperations txtLawTemplate;
    //邮件服务
    @Autowired
    MailService mailService
    /**
     * 增量更新
     */
    @Override
    void incrementalSync() {
        logger.info("法律法规增量更新开始")
        long startTime = System.currentTimeMillis();
        try{
            txtLawTemplate = mongoTemplatePool.getByName(MongoDBConfigConstants.TXT_LAW_DB)
            //法律法规状态修改为正在更新
            txtAndRrpStatusService.updateDsStatus(MongoDBConfigConstants.TXT_LAW_DB,TxtAndRrpStatusService.DS_STAS_UPDATE)
            //获取最新数据时间
            Query query=new Query();
            query.with(new Sort(new Sort.Order(Sort.Direction.DESC,"UPD_TIME")));
            Law law = txtLawTemplate.findOne(query,Law.class)
            if(law!=null){
                Date maxUpDate = law.UPD_TIME
                xmlRawSQLService.queryRawSqlByKey("selectLawsByMaxUpDate",[maxUpDate : maxUpDate],new TxtLawResultHandler(txtLawTemplate,Law.class,mailService))
            }
            //法律法规状态修改为正常，表示更新完成
            txtAndRrpStatusService.updateDsStatus(MongoDBConfigConstants.TXT_LAW_DB,TxtAndRrpStatusService.DS_STAS_NORMAL)
        }catch (err){
            logger.error("法律法规增量更新错误："+ err );
            txtAndRrpStatusService.updateDsStatus(MongoDBConfigConstants.TXT_LAW_DB, TxtAndRrpStatusService.DS_STAS_ERROR)
            mailService.sendMail("法律法规增增量更新失败","【法律法规】增量更新失败,错误信息："+err);
        }
        long endTime = System.currentTimeMillis();
        logger.info("法律法规增量更新完成,共耗时："+(endTime-startTime)+"ms");
    }

    /**
     * 全量更新
     */
    @Override
    void totalSync() {
          logger.info("法律法规初始化开始")
          long startTime = System.currentTimeMillis();
          try{
              txtLawTemplate = mongoTemplatePool.getByName(MongoDBConfigConstants.TXT_LAW_DB)
               //法律法规状态修改为正在更新
               txtAndRrpStatusService.updateDsStatus(MongoDBConfigConstants.TXT_LAW_DB,TxtAndRrpStatusService.DS_STAS_UPDATE)
              //判断法律法规临时表是否存在，存在则删除
              if(txtLawTemplate.collectionExists(MongoDBConfigConstants.TXT_LAW_DB+"_tmp")){
                  txtLawTemplate.dropCollection(MongoDBConfigConstants.TXT_LAW_DB+"_tmp")
              }
              //查询数据并保存到mongodb
              xmlRawSQLService.queryRawSqlByKey("selectAllLaw",new TxtLawResultHandler(txtLawTemplate,LawTmp.class,mailService))
               //将临时表更名为正常表
              txtLawTemplate.getCollection(MongoDBConfigConstants.TXT_LAW_DB+"_tmp").rename(MongoDBConfigConstants.TXT_LAW_DB,true)
               //保存属性类型
               savePros();
               //法律法规状态修改为正常，表示更新完成
               txtAndRrpStatusService.updateDsStatus(MongoDBConfigConstants.TXT_LAW_DB,TxtAndRrpStatusService.DS_STAS_NORMAL)
          }catch (err){
                logger.error("法律法规初始化错误："+ err );
                txtAndRrpStatusService.updateDsStatus(MongoDBConfigConstants.TXT_LAW_DB, TxtAndRrpStatusService.DS_STAS_ERROR)
                mailService.sendMail("法律法规初始化失败","【法律法规】初始化失败,错误信息："+err);
          }
           long endTime = System.currentTimeMillis();
           logger.info("法律法规初始化完成,共耗时："+(endTime-startTime)+"ms");
           mailService.sendMail("法律法规完成初始化扫描","【法律法规】初始化完成,共耗时："+(endTime-startTime)+"ms");
    }

    /**
     * 保存法律法规属性
     */
    void savePros(){
        List<LawPros> list = new ArrayList<LawPros>()
        list.add(new LawPros("ID","Decimal"))
        list.add(new LawPros("PUB_DT","DateTime"))
        list.add(new LawPros("TIT","String"))
        list.add(new LawPros("MKT_NAME","String"))
        list.add(new LawPros("COM_NAME","String"))
        list.add(new LawPros("PROV_DESC","String"))
        list.add(new LawPros("COM_ID","LongArray"))
        list.add(new LawPros("INDU_ID","LongArray"))
        list.add(new LawPros("CONT","String"))
        list.add(new LawPros("UPD_TIME","DateTime"))
        list.add(new LawPros("ENT_TIME","DateTime"))
        list.add(new LawPros("COMBINE_SEARCH","String"))
        list.add(new LawPros("INDU_NAME","String"))
        list.add(new LawPros("INFO_SOUR","String"))

        try{
            //判断属性集合是否存在，存在则删除
            if(txtLawTemplate.collectionExists(LawPros.class)){
                txtLawTemplate.dropCollection(LawPros.class);
            }
            //批量保存属性值
            txtLawTemplate.insertAll(list);
            logger.info("法律法规属性创建完成！")
        }catch (err){
            logger.error("法律法规属性创建错误："+err)
        }
    }
}
