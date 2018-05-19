package com.bigdata.datacenter.datasync.server

import com.bigdata.datacenter.datasync.service.ScanService
import com.bigdata.datacenter.datasync.service.impl.TxtNwsServiceNewImpl
import com.bigdata.datacenter.datasync.utils.constants.MongoDBConfigConstants
import org.springframework.context.support.ClassPathXmlApplicationContext

/**
 * 新闻信息扫描入口
 * Created by qq on 2017/5/19.
 */
class TxtNwsServiceNew {
    static ClassPathXmlApplicationContext context

    static main(args) {
        if (args == null || args.length < 2) {
            println("参数说明： -i 创建  -u 更新");
            return;
        }else if(args[0].toUpperCase().equals(MongoDBConfigConstants.MAIN_PARAM_INS)){
            TxtNwsServiceNewImpl.nwstyp = args[1].toUpperCase();
            context = new ClassPathXmlApplicationContext(MongoDBConfigConstants.XML_FILES)
            ScanService service = context.getBean("txt-nws")
            service.totalSync()
        }else if (args[0].toUpperCase().equals(MongoDBConfigConstants.MAIN_PARAM_UPD)) {
            TxtNwsServiceNewImpl.nwstyp = args[1].toUpperCase();
            List<String> list = new ArrayList<String>(Arrays.asList(MongoDBConfigConstants.XML_FILES))
            list.add("quartz/applicationContext-txtNwsQrtzNew.xml")
            context = new ClassPathXmlApplicationContext((String[])list.toArray())
        }
    }
}
