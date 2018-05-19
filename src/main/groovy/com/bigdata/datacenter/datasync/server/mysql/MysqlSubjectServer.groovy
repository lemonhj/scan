package com.bigdata.datacenter.datasync.server.mysql

import com.bigdata.datacenter.datasync.enums.ScanStateEnum
import com.bigdata.datacenter.datasync.enums.ScanTrigEnum
import com.bigdata.datacenter.datasync.service.ScanMonitorService
import com.bigdata.datacenter.datasync.service.ScanService
import com.bigdata.datacenter.datasync.service.SubjectStatusService
import com.bigdata.datacenter.datasync.service.impl.mysql.MysqlSubjectServiceImplNew
import com.bigdata.datacenter.datasync.utils.constants.MongoDBConfigConstants
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.context.support.ClassPathXmlApplicationContext

/**
 * mysql版 专题扫描入口类
 * Created by haiyangp on 2017/11/20.
 */
class MysqlSubjectServer {
    private static Logger logger = LoggerFactory.getLogger(MysqlSubjectServer.class);
    static ClassPathXmlApplicationContext context
    /**
     * 每一次触发，都对应一个TRIG_ID.这里的是rootTrigId,因为此处是静态的
     */
    static String TRIG_ID = NewTrigId()
    /**
     * 是否为第一次运行
     */
    static boolean IsFirstRun = true

    /**
     * 勽建立
     * @return
     */
    static String NewTrigId() {
        TRIG_ID = UUID.randomUUID().toString().replaceAll("-", "")
    }

    /**
     * 测试，1.-i 静态数据源:SECU_BASICINFO   分段: COM_LDR_HIS  增量：BD_TEST_ZL_1
     * @param args
     */
    static main(args) {
        if (args.length >= 2) {
            MysqlSubjectServiceImplNew.dsNms = Arrays.asList(args[1].split(","))
        }

        if (args == null || args.length == 0) {
            println("参数说明： -i 创建  -u 更新");
            return;
        } else if (args[0].toUpperCase().equals(MongoDBConfigConstants.MAIN_PARAM_INS)) {
            context = new ClassPathXmlApplicationContext(MongoDBConfigConstants.XML_FILES)

            ScanMonitorService scanMonitorService = context.getBean("scanMonitorService")
            scanMonitorService.initTable()
            //添加扫描监控数据
            scanMonitorService.addScanTrig(ScanTrigEnum.INIT)

            ScanService service = context.getBean("mysql-subject-new")
            service.totalSync()
            addShutdownHookMethod(context)
        } else if (args[0].toUpperCase().equals(MongoDBConfigConstants.MAIN_PARAM_UPD)) {
            List<String> list = new ArrayList<String>(Arrays.asList(MongoDBConfigConstants.XML_FILES))
            /**quartz方式下面这行**/
//            list.add("quartz/applicationContext-SubjectQrtzNew-mysql.xml")
            /**elasticJob方式用下面这行**/
            list.add("elasticjob/applicationContext-SubjectElasticJobNew-mysql.xml")

            context = new ClassPathXmlApplicationContext((String[]) list.toArray())

            ScanMonitorService scanMonitorService = context.getBean("scanMonitorService")
            scanMonitorService.initTable()

            ScanService service = context.getBean("mysql-subject-new")
            /**是否需要立即执行一次更新**/
//            service.incrementalSync()
            addShutdownHookMethod(context)
        }
    }

    /**
     * 添加程序被关闭时的方法
     * @param context spring上下文
     */
    static void addShutdownHookMethod(ClassPathXmlApplicationContext context) {
        //关闭时，将所有正在扫描的数据源状态记录修改为停止扫描
        Runtime.getRuntime().addShutdownHook(new MysqlSubjectShutdownThread(context))
    }

    /**
     * shutdown Thread
     */
    static class MysqlSubjectShutdownThread extends Thread {
        private ClassPathXmlApplicationContext context

        MysqlSubjectShutdownThread(ClassPathXmlApplicationContext context) {
            this.context = context
        }

        @Override
        void run() {
            logger.info("扫描程序关闭中...")
            ScanMonitorService scanMonitorService = context.getBean(ScanMonitorService.class)
            //将正在运行扫描的修改为结束
            scanMonitorService.updateCurrentAllDsState(ScanStateEnum.SCANING, ScanStateEnum.APP_EXIT, MysqlSubjectServer.TRIG_ID)
            //将正在等待的修改为结束
            scanMonitorService.updateCurrentAllDsState(ScanStateEnum.WAIT, ScanStateEnum.APP_EXIT, MysqlSubjectServer.TRIG_ID)
            logger.info("扫描程序关闭完成...")
        }
    }
}
