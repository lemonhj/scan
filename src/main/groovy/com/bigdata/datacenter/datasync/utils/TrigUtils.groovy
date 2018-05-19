package com.bigdata.datacenter.datasync.utils

/**
 * Created by K0090054 on 2018/2/1.
 */
class TrigUtils {
    static String getCurrentTrigId() {
        return TrigThreadContext.getCurrentTrigId()
    }

    /**
     * UUID建立
     * @return
     */
    static String genNewTrigId() {
        return UUID.randomUUID().toString().replaceAll("-", "")
    }
}
