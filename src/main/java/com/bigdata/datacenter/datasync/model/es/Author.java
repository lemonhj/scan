package com.bigdata.datacenter.datasync.model.es;

import java.io.Serializable;

/***
 * 作者po
 * @author lizhiwei
 *
 */
public class Author implements Serializable{
   
   private String author_code;
   private String author_name;
   
public String getAuthor_code() {
	return author_code;
}
public void setAuthor_code(String author_code) {
	this.author_code = author_code;
}
public String getAuthor_name() {
	return author_name;
}
public void setAuthor_name(String author_name) {
	this.author_name = author_name;
}
   

   
}
