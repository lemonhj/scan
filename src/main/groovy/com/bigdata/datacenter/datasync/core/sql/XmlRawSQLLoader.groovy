package com.bigdata.datacenter.datasync.core.sql

import org.springframework.stereotype.Component

/**
 * Created by Dean on 2017/5/18.
 */
@Component
class XmlRawSQLLoader {

    Map<String, String> sqlMap = new HashMap<>()
    List includeNodes = []

    public XmlRawSQLLoader(String sqlXmlPath) {
        initalizedScanSQL(sqlXmlPath)
    }

    public void initalizedScanSQL(String sqlXmlPath) {
//        def path = this.class.getClassLoader().getResource(sqlXmlPath).path
//        def sqlXml = new XmlParser().parse(path)
        def sqlXml = new XmlParser().parse(this.class.getClassLoader().getResourceAsStream(sqlXmlPath))
        sqlXml.mapper.each {
            def resPath = it.attribute("resource")
            mackupSqlMap(resPath)
        }
    }

    public void mackupSqlMap(String sqlXmlPath) {
//        def path = this.class.getClassLoader().getResource(sqlXmlPath).path
       // def sqlXml = new XmlParser().parse(path)
        def sqlXml = new XmlParser().parse(this.class.getClassLoader().getResourceAsStream(sqlXmlPath))
        sqlXml.select.each {
            if (it.include.size() == 0) {
                def key = it.attribute("id")
                def sql = it.text()
                sqlMap.put(key, sql)
            } else {
                def refId = it.include[0].attribute("refid")

                def sql = ""
                it.value().each { node ->
                    if (node instanceof Node && node.name() == "include") {
                        sql += getSqlNodeText(sqlXml, node.attribute("refid"))
                    } else if (node instanceof String){
                        sql += node
                    }
                }

                def key = it.attribute("id")
                sqlMap.put(key, sql)
            }
        }
    }

    public String getSqlNodeText(root, refId) {
        def node = root.sql.find {
            it.attribute("id") == refId
        }

        return node.text()
    }
}
