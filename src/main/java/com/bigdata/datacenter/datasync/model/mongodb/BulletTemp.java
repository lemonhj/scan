package com.bigdata.datacenter.datasync.model.mongodb;

import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.CompoundIndexes;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "TXT_BLT_LIST_tmp")
@CompoundIndexes({
	@CompoundIndex(name = "city_region_idx", def = "{'PUB_DT': -1, 'ENT_TIME': -1}")
})
public class BulletTemp extends Bullet {
}
