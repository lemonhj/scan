package com.bigdata.datacenter.datasync.server

import com.bigdata.datacenter.datasync.service.ScanService
import com.bigdata.datacenter.datasync.utils.constants.MongoDBConfigConstants
import org.springframework.context.support.ClassPathXmlApplicationContext

/**
 * 法律法规扫描启动入口
 * Created by qq on 2017/5/19.
 */
class RrpBasServer {
    static ClassPathXmlApplicationContext context

    static main(args) {
        if (args == null || args.length == 0) {
            println("参数说明： -i 创建  -u 更新");
            return;
        }else if(args[0].toUpperCase().equals(MongoDBConfigConstants.MAIN_PARAM_INS)){
            context = new ClassPathXmlApplicationContext(MongoDBConfigConstants.XML_FILES)
            ScanService service = context.getBean("rrp-bas")
            service.totalSync()
        }else if (args[0].toUpperCase().equals(MongoDBConfigConstants.MAIN_PARAM_UPD)) {
            List<String> list = new ArrayList<String>(Arrays.asList(MongoDBConfigConstants.XML_FILES))
            list.add("quartz/applicationContext-rrpBasQrtz.xml")
            context = new ClassPathXmlApplicationContext((String[])list.toArray())
        }
    }
}
