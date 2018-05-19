package com.bigdata.datacenter.datasync.model.es;

import java.io.Serializable;

/*****
 *附件 
 * @author lizhiwei
 *
 */

public class Attachment implements Serializable {

	private static final long serialVersionUID = -5836632939419325290L;
	private String fileurl;
	private String filename;
	private String fileid;
	private String fileSize;
	private String pageCounts;
	
	public String getFileurl() {
		return fileurl;
	}
	public void setFileurl(String fileurl) {
		this.fileurl = fileurl;
	}
	public String getFilename() {
		return filename;
	}
	public void setFilename(String filename) {
		this.filename = filename;
	}
	public String getFileid() {
		return fileid;
	}
	public void setFileid(String fileid) {
		this.fileid = fileid;
	}

	public String getFileSize() {
		return fileSize;
	}
	public void setFileSize(String fileSize) {
		this.fileSize = fileSize;
	}
	public String getPageCounts() {
		return pageCounts;
	}
	public void setPageCounts(String pageCounts) {
		this.pageCounts = pageCounts;
	}
	
}
