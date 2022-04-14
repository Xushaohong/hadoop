package com.tencent.mdfs.provider.dto;

import java.util.Date;

public class AppDirRelationDto {
  private Integer id;

  private Long appId;

  private Long dirId;

  private Byte type;

  private Date createTime;

  private Date updateTime;

  private Byte state;

  public void setId(Integer id) {
    this.id=id;
  }

  public Integer getId() {
    return this.id;
  }

  public void setAppId(Long appId) {
    this.appId=appId;
  }

  public Long getAppId() {
    return this.appId;
  }

  public void setDirId(Long dirId) {
    this.dirId=dirId;
  }

  public Long getDirId() {
    return this.dirId;
  }

  public void setType(Byte type) {
    this.type=type;
  }

  public Byte getType() {
    return this.type;
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
    return "AppDirRelationDto{"+
        "id=" + id  + 
        ", appId=" + appId  + 
        ", dirId=" + dirId  + 
        ", type=" + type  + 
        ", createTime=" + createTime  + 
        ", updateTime=" + updateTime  + 
        ", state=" + state  + 
        "}";
  }
}
