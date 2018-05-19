package com.bigdata.datacenter.datasync.server

import com.bigdata.datacenter.datasync.service.ScanService
import com.bigdata.datacenter.datasync.service.impl.MailService
import org.springframework.context.support.ClassPathXmlApplicationContext

/**
 * Created by Dean on 2017/5/15.
 */
class TxtBltServer {
    static ClassPathXmlApplicationContext context

    static main(args) {
        String[] xmlFiles = [
                "applicationContext-dataSource.xml",
                "applicationContext-ebean.xml",
                "applicationContext-mongo.xml",
                "applicationContext-spring.xml"]

        context = new ClassPathXmlApplicationContext(xmlFiles)
        ScanService service = context.getBean("txt-bitNew")
        service.totalSync()
//        service.incrementalSync()
    }
}
