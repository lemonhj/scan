package com.bigdata.datacenter.datasync.server

import com.bigdata.datacenter.datasync.service.ScanService
import com.bigdata.datacenter.datasync.service.impl.AllIndexServiceImpl
import com.bigdata.datacenter.datasync.service.impl.AllIndexServiceImplNew
import com.bigdata.datacenter.datasync.utils.PropertiesUtil
import com.bigdata.datacenter.datasync.utils.constants.MongoDBConfigConstants
import org.apache.commons.lang.StringUtils
import org.springframework.context.support.ClassPathXmlApplicationContext

/**
 * Created by Dan on 2017/6/20.
 */
class AllIndexServer {
    static ClassPathXmlApplicationContext context

    static main(args){
        if (StringUtils.isNotEmpty(PropertiesUtil.getProperty("index.include"))) {
            for(String ds: PropertiesUtil.getProperty("index.include").split(",")){
                if(StringUtils.isNotEmpty(ds)) {
                    AllIndexServiceImplNew.m_IncludeDS.add(ds.toUpperCase())
                }
            }
        }
        if (StringUtils.isNotEmpty(PropertiesUtil.getProperty("index.exclude"))) {
            for(String ds: PropertiesUtil.getProperty("index.exclude").split(",")){
                if(StringUtils.isNotEmpty(ds)) {
                    AllIndexServiceImplNew.m_ExcludeDS.add(ds.toUpperCase())
                }
            }
        }
        if (args.length >= 2) {
            AllIndexServiceImplNew.dsNms = Arrays.asList(args[1].split(","))
        }

        if (args == null || args.length == 0) {
            println("参数说明： -i 创建  -u 更新");
            return;
        }else if(args[0].toUpperCase().equals(MongoDBConfigConstants.MAIN_PARAM_INS)){
            context = new ClassPathXmlApplicationContext(MongoDBConfigConstants.XML_FILES)
            ScanService service = context.getBean("all-index-page")
            service.totalSync()
        }else if (args[0].toUpperCase().equals(MongoDBConfigConstants.MAIN_PARAM_UPD)) {
            List<String> list = new ArrayList<String>(Arrays.asList(MongoDBConfigConstants.XML_FILES))
            list.add("quartz/applicationContext-IndexQrtzNew.xml")
            context = new ClassPathXmlApplicationContext((String[])list.toArray())
        }
    }
}
