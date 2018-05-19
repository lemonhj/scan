package com.bigdata.datacenter.datasync.model.es;

import java.io.Serializable;
import java.util.Date;
import java.util.List;


/***
 * 资讯po
 * @author lizhiwei
 *
 */
public class CbdNews implements Serializable{

private static final long serialVersionUID = 5740323067760966857L;
	
	
   private String content;
   private String description;
   private String data_type;
   private String orig_id;
   private Date pub_date;
  
   private String source_code;
   private String source_name;
   private String title;
   private List<Author> authors;
   private List<Attachment> files;
   private List<Industry> industries;
   private List<Security> securities;
     
   private Date upd_time;
   private String mkt_name;
   private String com_name;
   private String com_id;
   private Date es_time;
   private String category;
   private List<Image> images;

   
public String getData_type() {
	return data_type;
}
public void setData_type(String data_type) {
	this.data_type = data_type;
}

public Date getUpd_time() {
	return upd_time;
}
public void setUpd_time(Date upd_time) {
	this.upd_time = upd_time;
}
public String getMkt_name() {
	return mkt_name;
}
public void setMkt_name(String mkt_name) {
	this.mkt_name = mkt_name;
}
public String getCom_name() {
	return com_name;
}
public void setCom_name(String com_name) {
	this.com_name = com_name;
}

public List<Security> getSecurities() {
	return securities;
}
public void setSecurities(List<Security> securities) {
	this.securities = securities;
}
public List<Industry> getIndustries() {
	return industries;
}
public void setIndustries(List<Industry> industries) {
	this.industries = industries;
}


public String getContent() {
	return content;
}
public void setContent(String content) {
	this.content = content;
}
public String getDescription() {
	return description;
}
public void setDescription(String description) {
	this.description = description;
}
   

public String getCategory() {
	return category;
}
public void setCategory(String category) {
	this.category = category;
}
public String getOrig_id() {
	return orig_id;
}
public void setOrig_id(String orig_id) {
	this.orig_id = orig_id;
}
public Date getPub_date() {
	return pub_date;
}

public void setPub_date(Date pub_date) {
	this.pub_date = pub_date;
}

public String getCom_id() {
	return com_id;
}
public void setCom_id(String com_id) {
	this.com_id = com_id;
}
public String getSource_code() {
	return source_code;
}
public void setSource_code(String source_code) {
	this.source_code = source_code;
}
public String getSource_name() {
	return source_name;
}
public void setSource_name(String source_name) {
	this.source_name = source_name;
}
public String getTitle() {
	return title;
}
public void setTitle(String title) {
	this.title = title;
}



public List<Image> getImages() {
	return images;
}
public void setImages(List<Image> images) {
	this.images = images;
}



private String ext_obj_0;
private String ext_obj_1;
private String ext_obj_2;
private String ext_obj_3;
private String ext_obj_4;
private String ext_obj_5;
private String ext_obj_6;
private String ext_obj_7;
private String ext_obj_8;
private String ext_obj_9;


public List<Author> getAuthors() {
	return authors;
}
public void setAuthors(List<Author> authors) {
	this.authors = authors;
}

public Date getEs_time() {
	return es_time;
}
public void setEs_time(Date es_time) {
	this.es_time = es_time;
}
public List<Attachment> getFiles() {
	return files;
}
public void setFiles(List<Attachment> files) {
	this.files = files;
}
public String getExt_obj_0() {
	return ext_obj_0;
}
public void setExt_obj_0(String ext_obj_0) {
	this.ext_obj_0 = ext_obj_0;
}
public String getExt_obj_1() {
	return ext_obj_1;
}
public void setExt_obj_1(String ext_obj_1) {
	this.ext_obj_1 = ext_obj_1;
}
public String getExt_obj_2() {
	return ext_obj_2;
}
public void setExt_obj_2(String ext_obj_2) {
	this.ext_obj_2 = ext_obj_2;
}
public String getExt_obj_3() {
	return ext_obj_3;
}
public void setExt_obj_3(String ext_obj_3) {
	this.ext_obj_3 = ext_obj_3;
}
public String getExt_obj_4() {
	return ext_obj_4;
}
public void setExt_obj_4(String ext_obj_4) {
	this.ext_obj_4 = ext_obj_4;
}
public String getExt_obj_5() {
	return ext_obj_5;
}
public void setExt_obj_5(String ext_obj_5) {
	this.ext_obj_5 = ext_obj_5;
}
public String getExt_obj_6() {
	return ext_obj_6;
}
public void setExt_obj_6(String ext_obj_6) {
	this.ext_obj_6 = ext_obj_6;
}
public String getExt_obj_7() {
	return ext_obj_7;
}
public void setExt_obj_7(String ext_obj_7) {
	this.ext_obj_7 = ext_obj_7;
}
public String getExt_obj_8() {
	return ext_obj_8;
}
public void setExt_obj_8(String ext_obj_8) {
	this.ext_obj_8 = ext_obj_8;
}
public String getExt_obj_9() {
	return ext_obj_9;
}
public void setExt_obj_9(String ext_obj_9) {
	this.ext_obj_9 = ext_obj_9;
}



   

   
   
}
