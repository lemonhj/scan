package com.bigdata.datacenter.datasync.utils
/**
 * Created by haiyangp on 2017/11/24.
 */
class MysqlUtils {
    /**
     * 获取索引数组
     * @param dsIndexLst
     * @return
     */
    static List<String[]> getIndexArray(List<Map<String, Object>> dsIndexLst) {
        if (dsIndexLst == null || dsIndexLst.size() == 0) {
            return null
        }
        def tempList = dsIndexLst.groupBy {
            it.INDEX_NAME
        }.collectEntries { k, v ->
            [(k): v.collect {
                it.PROP_NAME
            }]
        }
        List<String[]> resultList = new ArrayList<>()
        tempList.each { resultList.add(it.value) }
        return resultList
    }

    static void main(String[] args) {
        List<Map<String, Object>> dsIndexLst = new ArrayList<>()
        Map<String, Object> map = new HashMap<>()
        map.put("INDEX_NAME", "索引一")
        map.put("PROP_NAME", "aaaaaa")
        dsIndexLst.add(map)
        Map<String, Object> map1 = new HashMap<>()
        map1.put("INDEX_NAME", "索引一")
        map1.put("PROP_NAME", "bbbbb")
        dsIndexLst.add(map1)
        Map<String, Object> map2 = new HashMap<>()
        map2.put("INDEX_NAME", "索引一")
        map2.put("PROP_NAME", "cccccc")
        dsIndexLst.add(map2)
        Map<String, Object> map3 = new HashMap<>()
        map3.put("INDEX_NAME", "索引二")
        map3.put("PROP_NAME", "ddddd")
        dsIndexLst.add(map3)
        Map<String, Object> map4 = new HashMap<>()
        map4.put("INDEX_NAME", "索引二")
        map4.put("PROP_NAME", "eeeee")
        dsIndexLst.add(map4)
        Map<String, Object> map5 = new HashMap<>()
        map5.put("INDEX_NAME", "索引三")
        map5.put("PROP_NAME", "fffff")
        dsIndexLst.add(map5)

        List<String[]> resultList = getIndexArray(dsIndexLst)
        for (String[] indItem : resultList) {
            println("--->" + indItem)
        }
    }
}
