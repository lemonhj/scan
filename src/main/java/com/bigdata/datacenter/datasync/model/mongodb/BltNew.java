package com.bigdata.datacenter.datasync.model.mongodb;

import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.CompoundIndexes;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * Created by Dan on 2017/6/13.
 */
@Document(collection = "TXT_BLT_LIST_tmp")
@CompoundIndexes({
		@CompoundIndex(name = "blt_pub_end_idx", def = "{'PUB_DT': -1, 'ENT_TIME': -1}"),
		/*@CompoundIndex(name = "ID", def = "{'ID': -1}"),
		@CompoundIndex(name = "TYP_CODE", def = "{'TYP_CODE': NULL}"),
		@CompoundIndex(name = "SECU_ID", def = "{'SECU_ID': -1}"),
		@CompoundIndex(name = "UPD_TIME", def = "{'UPD_TIME': -1}"),
		@CompoundIndex(name = "COM_ID", def = "{'COM_ID': -1}"),*/
})
//public class BltNew extends HashMap<String,Object> {
public class BltNew extends Bullet {
}
