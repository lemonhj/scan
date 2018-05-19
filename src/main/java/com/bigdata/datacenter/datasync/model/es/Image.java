package com.bigdata.datacenter.datasync.model.es;

import java.io.Serializable;

/*****
 *图片
 * @author lizhiwei
 *
 */

public class Image implements Serializable {
	
	
	private static final long serialVersionUID = -2695738718069987022L;
	private String imageurl;
	private String imagename;
	private String imageid;
	public String getImageurl() {
		return imageurl;
	}
	public void setImageurl(String imageurl) {
		this.imageurl = imageurl;
	}
	public String getImagename() {
		return imagename;
	}
	public void setImagename(String imagename) {
		this.imagename = imagename;
	}
	public String getImageid() {
		return imageid;
	}
	public void setImageid(String imageid) {
		this.imageid = imageid;
	}
	
	
	
}
