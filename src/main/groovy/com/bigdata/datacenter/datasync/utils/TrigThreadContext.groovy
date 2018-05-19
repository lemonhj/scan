package com.bigdata.datacenter.datasync.utils

/**
 * Created by K0090054 on 2018/2/1.
 */
class TrigThreadContext {
    static ThreadLocal<String> currentTrigId = new ThreadLocal<>()

    static String getCurrentTrigId() {
        currentTrigId.get()
    }

    static String bindCurrentTrigId(String trigId) {
        currentTrigId.set(trigId)
    }

}
