package com.tencent.mdfs.provider.dto;

import java.util.Date;

public class AppInfoDto {

  private Long appId;

  private String appName;

  private String appCode;

  private Byte type;

  private String bg;

  private String product;

  private String business;

  private String owner;

  private Date createTime;

  private Date updateTime;

  private Byte state;
  public void setAppId(Long appId) {
    this.appId=appId;
  }

  public Long getAppId() {
    return this.appId;
  }

  public void setAppName(String appName) {
    this.appName=appName;
  }

  public String getAppName() {
    return this.appName;
  }

  public void setAppCode(String appCode) {
    this.appCode=appCode;
  }

  public String getAppCode() {
    return this.appCode;
  }

  public void setType(Byte type) {
    this.type=type;
  }

  public Byte getType() {
    return this.type;
  }

  public void setBg(String bg) {
    this.bg=bg;
  }

  public String getBg() {
    return this.bg;
  }

  public void setProduct(String product) {
    this.product=product;
  }

  public String getProduct() {
    return this.product;
  }

  public void setBusiness(String business) {
    this.business=business;
  }

  public String getBusiness() {
    return this.business;
  }

  public void setOwner(String owner) {
    this.owner=owner;
  }

  public String getOwner() {
    return this.owner;
  }

  public void setCreateTime(Date createTime) {
    this.createTime=createTime;
  }

  public Date getCreateTime() {
    return this.createTime;
  }

  public void setUpdateTime(Date updateTime) {
    this.updateTime=updateTime;
  }

  public Date getUpdateTime() {
    return this.updateTime;
  }

  public void setState(Byte state) {
    this.state=state;
  }

  public Byte getState() {
    return this.state;
  }

  public String toString() {
    return "AppInfoDto{"+
        ", appId=" + appId  + 
        ", appName=" + appName  + 
        ", appCode=" + appCode  + 
        ", type=" + type  + 
        ", bg=" + bg  + 
        ", product=" + product  + 
        ", business=" + business  + 
        ", owner=" + owner  + 
        ", createTime=" + createTime  + 
        ", updateTime=" + updateTime  + 
        ", state=" + state  + 
        "}";
  }
}
