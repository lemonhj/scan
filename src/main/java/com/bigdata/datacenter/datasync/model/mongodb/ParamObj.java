package com.bigdata.datacenter.datasync.model.mongodb;

import java.io.Serializable;

public class ParamObj implements Serializable,Comparable<ParamObj> {
	private String name;
	private Long type;
	private String description;
	private String defaultValue;
	private String defaultValueName;
	
	public String getDefaultValueName() {
		return defaultValueName;
	}
	public void setDefaultValueName(String defaultValueName) {
		this.defaultValueName = defaultValueName;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public Long getType() {
		return type;
	}
	public void setType(Long type) {
		this.type = type;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public String getDefaultValue() {
		return defaultValue;
	}
	public void setDefaultValue(String defaultValue) {
		this.defaultValue = defaultValue;
	}
	
	@Override
	public int compareTo(ParamObj obj) {
		return this.getName().compareTo(obj.getName());
	}
}
