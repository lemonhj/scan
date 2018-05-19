package com.bigdata.datacenter.datasync.utils;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import com.bigdata.datacenter.datasync.model.es.CbdNews;

/****
 * es工具类
 * @author lizhiwei
 *
 */

public class ESearchHelper {
	private static String clusterName = null;
	private static String clusterServerip = null; 
	private static String indexName = null;
	private static int port = 9300;
	private static TransportClient client = null;
	private static final Logger logger = Logger.getLogger(ESearchHelper.class);
	
	static {
		try {
			  if(null == clusterName) {
				  Properties props = new Properties();
				  InputStream in = ESearchHelper.class.getResourceAsStream("/elas.properties");
				  props.load(in);
				  clusterName = props.getProperty("cluster_name");
				  clusterServerip = props.getProperty("cluster_serverip");
				  indexName = props.getProperty("indexname"); 
				  port = props.getProperty("port") == null?port:Integer.parseInt(props.getProperty("port"));
			  }
		} catch (IOException e) {
			logger.error("io error",e);
		}
	}

	
/****
 * 获取es连接	
 * @return
 */
	@SuppressWarnings("unchecked")
	private static TransportClient getClient() {
		if(client != null) return client;
		Settings settings = Settings.builder().put("cluster.name", clusterName).build();
		client = new PreBuiltTransportClient(settings);
		try {
			client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(clusterServerip), port));
			// clientMaps.put(1, client);
		} catch (UnknownHostException e) {
			logger.error("es连接失败！",e);
		} catch (Exception e) {
			logger.error("es未知异常！",e);
		}
		return client;
	}
	

	@SuppressWarnings("unchecked")
	private static XContentBuilder createMapJson(Map<String, Object> mapParam) throws Exception {
		XContentBuilder source = XContentFactory.jsonBuilder().startObject();
		for (Map.Entry<String, Object> entry : mapParam.entrySet()) {
           source.field(entry.getKey(), entry.getValue());
		}
		source.endObject();
		return source;
	}


	private static XContentBuilder createMapJson2(Map<String, String> mapParam) throws Exception {
		XContentBuilder source = XContentFactory.jsonBuilder().startObject();
		for (Map.Entry<String, String> entry : mapParam.entrySet()) {
			source.field(entry.getKey(), entry.getValue());
		}
		source.endObject();
		return source;
	}

	
/***
 * 添加文档
 * @param news 
 * @param types
 * @return
 * @throws Exception
 */
    private  boolean addDocNew(CbdNews news, String types) throws Exception{
    	Client client = getClient();
    	try{
    		ObjectMapper mapper = new ObjectMapper();
    		byte[] content = mapper.writeValueAsBytes(news);
        	IndexResponse response = null;
    		if (news.getOrig_id() == null) {
    			response = client.prepareIndex(indexName,types).setSource(content).get();
    		} else {
    			response = client.prepareIndex(indexName, types, news.getOrig_id()).setSource(content).get();
    		}
    		logger.info("response:"+response.toString());
    	} catch(Exception ex){
    		logger.error("orgid:"+news.getOrig_id());
    		logger.error("addDocNew error!", ex);
    		return false;
    	} finally{
    		/*if(client != null){
    			client.close();
    		}*/
    	}
		return true;

    }
    
    
	@SuppressWarnings("unused")
	private  boolean addDocNew(CbdNews news,Client client) throws Exception{
    	try{
    		ObjectMapper mapper = new ObjectMapper();
    		byte[] content = mapper.writeValueAsBytes(news);
        	IndexResponse response = null;
    		if (news.getOrig_id() == null) {
    			response = client.prepareIndex(indexName,"cbdnews").setSource(content).get();
    		} else {
    			response = client.prepareIndex(indexName, "cbdnews", news.getOrig_id()).setSource(content).get();
    		}
    		logger.info("response:"+response.toString());
    	} catch(Exception ex){
    		logger.error("addDocNew error!", ex);
    		return false;
    	}
		return true;

    }
	
	
	public static void closeClient(){
		if(client != null){
			client.close();
			client = null;
		}
	}
	
	
}
