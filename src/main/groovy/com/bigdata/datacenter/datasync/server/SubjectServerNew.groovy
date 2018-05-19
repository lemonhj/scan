package com.bigdata.datacenter.datasync.server

import com.bigdata.datacenter.datasync.service.ScanService
import com.bigdata.datacenter.datasync.service.impl.SubjectServiceImplNew
import com.bigdata.datacenter.datasync.utils.constants.MongoDBConfigConstants
import org.springframework.context.support.ClassPathXmlApplicationContext

/**
 * 专题数据源扫描入口
 * Created by qq on 2017/5/19.
 */
class SubjectServerNew {
    static ClassPathXmlApplicationContext context

    //job当前数量
    public static int jobNum = 0;

    static main(args) {
        if (args == null || args.length == 0) {
            println("参数说明： -i 创建  -u 更新");
            return;
        }else if(args[0].toUpperCase().equals(MongoDBConfigConstants.MAIN_PARAM_INS)){
            if (args.length >= 2) {
                SubjectServiceImplNew.dsNms = Arrays.asList(args[1].split(","));
            }
            context = new ClassPathXmlApplicationContext(MongoDBConfigConstants.XML_FILES)
            ScanService service = context.getBean("subject-new")
            service.resetDsStats();
            service.totalSync()
        }else if (args[0].toUpperCase().equals(MongoDBConfigConstants.MAIN_PARAM_UPD)) {
            List<String> list = new ArrayList<String>(Arrays.asList(MongoDBConfigConstants.XML_FILES))
            list.add("quartz/applicationContext-SubjectQrtzNew.xml")
            context = new ClassPathXmlApplicationContext((String[])list.toArray())
            ScanService service = context.getBean("subject-new")
            service.resetDsStats();
        }
    }
}
