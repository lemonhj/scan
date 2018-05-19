package com.bigdata.datacenter.datasync.model.mongodb;

import org.springframework.data.mongodb.core.mapping.Document;

import java.util.HashMap;

/**
 * Created by Dan on 2017/6/26.
 */
@Document(collection = "IDX_STATUS")
public class DsStatus extends HashMap {
}
