package com.bigdata.datacenter.datasync.utils

import org.springframework.data.mongodb.core.query.Update

/**
 * Created by happy on 2015/11/12.
 */
class EntityUtil {
    static def setPropertyByMap(Object entity, Map map){
        if(map && entity) {
            entity.properties.each {name, value->
                entity."${name}" = map[name]
            }
        }
    }

    static def mapToObject(Map map, Class clazz) {
        def instance = clazz.newInstance()

        map.each { key, value ->
            key = key.toUpperCase()
            if (instance.hasProperty(key) != null) {
                instance[key] = value
            }
        }

        return instance
    }

    static def objectToUpdate(Object obj) {
        Update update = new Update()
        if(obj) {
            if(obj instanceof  Map){
                obj.each {
                    update.set(it.key,it.value)
                }
            }else{
                obj.properties.each {name, value->
                    if(name !="class"){
                        update.set(name,value)
                    }
                }
            }
        }
        return update
    }
}
