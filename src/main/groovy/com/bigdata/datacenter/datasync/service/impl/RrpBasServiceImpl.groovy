package com.bigdata.datacenter.datasync.service.impl

import com.bigdata.datacenter.datasync.core.data.MongoTemplatePool
import com.bigdata.datacenter.datasync.service.ScanService
import com.bigdata.datacenter.datasync.service.TxtAndRrpStatusService
import com.bigdata.datacenter.datasync.service.XmlRawSQLService
import com.bigdata.datacenter.datasync.utils.DateUtils
import com.bigdata.datacenter.datasync.utils.constants.MongoDBConfigConstants
import com.bigdata.datacenter.datasync.model.mongodb.ReportBas
import com.bigdata.datacenter.datasync.model.mongodb.ReportBasTmp
import com.bigdata.datacenter.datasync.model.mongodb.ReportPros
import com.bigdata.datacenter.metadata.service.RawSQLService
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.data.domain.Sort
import org.springframework.data.mongodb.core.MongoOperations
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.stereotype.Service
import com.bigdata.datacenter.datasync.utils.ESearchHelper

/**
 *  研报
 * Created by qq on 2017/5/19.
 */
@Service(value = "rrp-bas")
class RrpBasServiceImpl implements ScanService{
    private static final Logger logger = Logger.getLogger(RrpBasServiceImpl.class)
    @Autowired
    RawSQLService rawSQLService
    @Autowired
    XmlRawSQLService xmlRawSQLService
    @Autowired
    TxtAndRrpStatusService txtAndRrpStatusService
    @Autowired
    MongoTemplatePool mongoTemplatePool
    MongoOperations rrpBasTemplate

    /**
     * 增量更新
     */
    @Override
    void incrementalSync() {
        logger.info("研报增量更新开始")
        long startTime = System.currentTimeMillis();
        try{
            rrpBasTemplate = mongoTemplatePool.getByName(MongoDBConfigConstants.RRP_BAS_DB)
            //研报状态修改为正在更新
            txtAndRrpStatusService.updateDsStatus(MongoDBConfigConstants.RRP_BAS_DB,TxtAndRrpStatusService.DS_STAS_UPDATE)
            //获取最新数据时间
            Query query=new Query();
            query.with(new Sort(new Sort.Order(Sort.Direction.DESC,"UPD_TIME")));
            ReportBas rrpbas = rrpBasTemplate.findOne(query,ReportBas.class)
            if(rrpbas!=null){
                Date maxUpDate = rrpbas.UPD_TIME
                /*因需求不理解尚未实现*/
//                reGetRelateData(maxUpDate,col);
                xmlRawSQLService.queryRawSqlByKey("selectRrpBasByMaxId",[maxUpDate : maxUpDate],new RrpBasResultHandler(rrpBasTemplate, ReportBas.class, xmlRawSQLService))
            }
            //研报状态修改为正常，表示更新完成
            txtAndRrpStatusService.updateDsStatus(MongoDBConfigConstants.RRP_BAS_DB,TxtAndRrpStatusService.DS_STAS_NORMAL)
        }catch (err){
            logger.error("研报定時同步更新错误："+ err );
            txtAndRrpStatusService.updateDsStatus(MongoDBConfigConstants.RRP_BAS_DB, TxtAndRrpStatusService.DS_STAS_ERROR)
        } finally {
            try{
                ESearchHelper.closeClient();
            }catch (err){}
		}
        long endTime = System.currentTimeMillis();
        logger.info("研报增量更新完成,共耗时："+(endTime-startTime)+"ms");
    }

    /**
     * 全量更新
     */
    @Override
    void totalSync() {
          logger.info("研报全量更新开始")
          long startTime = System.currentTimeMillis();
          try{
              rrpBasTemplate = mongoTemplatePool.getByName(MongoDBConfigConstants.RRP_BAS_DB)
               //研报状态修改为正在更新
               txtAndRrpStatusService.updateDsStatus(MongoDBConfigConstants.RRP_BAS_DB,TxtAndRrpStatusService.DS_STAS_UPDATE)
              //判断研报临时表是否存在，存在则删除
              if(rrpBasTemplate.collectionExists(MongoDBConfigConstants.RRP_BAS_DB+"_tmp")){
                  rrpBasTemplate.dropCollection(MongoDBConfigConstants.RRP_BAS_DB+"_tmp")
              }
			  //查询数据并保存到mongodb  
              xmlRawSQLService.queryRawSqlByKey("selectAllRrpBas",new RrpBasResultHandler(rrpBasTemplate, ReportBasTmp.class, xmlRawSQLService))
               //将临时表更名为正常表
              rrpBasTemplate.getCollection(MongoDBConfigConstants.RRP_BAS_DB+"_tmp").rename(MongoDBConfigConstants.RRP_BAS_DB,true)
               //保存属性类型
               savePros();
               //研报状态修改为正常，表示更新完成
               txtAndRrpStatusService.updateDsStatus(MongoDBConfigConstants.RRP_BAS_DB,TxtAndRrpStatusService.DS_STAS_NORMAL)
            }catch (err){
                logger.error("研报全量更新错误："+ err );
                txtAndRrpStatusService.updateDsStatus(MongoDBConfigConstants.RRP_BAS_DB, TxtAndRrpStatusService.DS_STAS_ERROR)
            } finally {  
		        ESearchHelper.closeClient();
		    }
           long endTime = System.currentTimeMillis();
           logger.info("研报全量更新完成,共耗时："+(endTime-startTime)+"ms");
    }

    /**
     * 删除数据库中已删除的数据
     * @param maxUpDate
     */
    void removeDate(){
        logger.info("删除研报数据开始")
        long start = System.currentTimeMillis()
        //获取最新数据时间
        Query query=new Query();
        query.with(new Sort(new Sort.Order(Sort.Direction.DESC,"UPD_TIME")));
        ReportBas rrpbas = rrpBasTemplate.findOne(query,ReportBas.class)
        if(rrpbas!=null){
            Date maxUpDate = rrpbas.UPD_TIME
            def idsList = xmlRawSQLService.queryRawSqlByKey("selectRrpIdByMaxUpDate",[maxUpDate : maxUpDate])
            List<Double> ids = mapToDouble(idsList)
            Date condDate = DateUtils.string2Date(DateUtils.date2String(maxUpDate, DateUtils.FORMAT_DATE), DateUtils.FORMAT_DATE);
            List<ReportBas> rrpList = rrpBasTemplate.find(new Query(Criteria.where("UPD_TIME").gte(condDate)),ReportBas.class)
            rrpList.each {
                if(!ids.contains(it.ID)){
                    rrpBasTemplate.remove(it)
                }
            }
        }
        long end = System.currentTimeMillis()
        logger.info("删除研报数据完成，耗时："+(end-start)+"ms")
    }

    /**
     * 保存研报属性
     */
    void savePros(){
        List<ReportPros> list = new ArrayList<ReportPros>()
        list.add(new ReportPros("ABST_SHT","String"))
        list.add(new ReportPros("AREA_CODE","Decimal"))
        list.add(new ReportPros("AUT","String"))
        list.add(new ReportPros("COM_ID","Decimal"))
        list.add(new ReportPros("COM_NAME","String"))
        list.add(new ReportPros("EXCH_CODE","Decimal"))
        list.add(new ReportPros("ID","Decimal"))
        list.add(new ReportPros("IS_WTR_MARK","String"))
        list.add(new ReportPros("KEYW","String"))
        list.add(new ReportPros("LANG_TYP","Decimal"))
        list.add(new ReportPros("OBJ_CODE","Decimal"))
        list.add(new ReportPros("PUB_DT","DateTime"))
        list.add(new ReportPros("RPT_DEG","Decimal"))
        list.add(new ReportPros("RPT_LVL","Decimal"))
        list.add(new ReportPros("RPT_TYP_CODE","Decimal"))
        list.add(new ReportPros("SECT_CODE","Decimal"))
        list.add(new ReportPros("SUBJ_CODE","String"))
        list.add(new ReportPros("SUB_TIT","String"))
        list.add(new ReportPros("TIT","String"))
        list.add(new ReportPros("WRT_DT","DateTime"))
        list.add(new ReportPros("UPD_TIME","DateTime"))
        list.add(new ReportPros("ENT_TIME","DateTime"))
        list.add(new ReportPros("INDU_ID","LongArray"))
        list.add(new ReportPros("SECU_ID","LongArray"))
        list.add(new ReportPros("FLD_VAL","LongArray"))
        list.add(new ReportPros("INDU_RAT_ORIG_DESC","String"))
        list.add(new ReportPros("INDU_RAT_ORIG_DESC_LST","String"))
        list.add(new ReportPros("RAT_ORIG_DESC","String"))
        list.add(new ReportPros("RAT_ORIG_DESC_LST","String"))
        list.add(new ReportPros("TARG_PRC_MIN","Decimal"))
        list.add(new ReportPros("TARG_PRC_MAX","Decimal"))
        list.add(new ReportPros("CST_DESC","String"))

        try{
            //判断属性集合是否存在，存在则删除
            if(rrpBasTemplate.collectionExists(ReportPros.class)){
                rrpBasTemplate.dropCollection(ReportPros.class);
            }
            //批量保存属性值
            rrpBasTemplate.insertAll(list);
            logger.info("研报属性创建完成！")
        }catch (err){
            logger.error("研报属性创建错误："+err)
        }
    }

    /**
     * 将对象转换为Double
     * @param result
     * @return
     */
    List<Long> mapToDouble(List<Map<String,Object>> result){
        if(result!=null&&result.size()>0){
            List<Double> list = new ArrayList<Double>()
            for (Map<String,Object> map : result){
                list.add((Double)(map.values()).getAt(0))
            }
            return list
        }else{
            return null
        }
    }
}
