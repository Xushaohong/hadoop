package com.tencent.mdfs.provider.dto;

import java.util.Date;

public class DirInfoDto {

  private String clusterId;

  private Long dirId;

  private Integer appId;

  private String logicPath;

  private String realPath;

  private Byte type;

  private Byte shareType;

  private String namenodeAddr;

  private Date createTime;

  private Date updateTime;

  private Byte state;


  /**
   * 跨集群地址
   */
  private String clusterInfos;

  public void setDirId(Long dirId) {
    this.dirId=dirId;
  }

  public Long getDirId() {
    return this.dirId;
  }

  public void setAppId(Integer appId) {
    this.appId=appId;
  }

  public Integer getAppId() {
    return this.appId;
  }

  public void setLogicPath(String logicPath) {
    this.logicPath=logicPath;
  }

  public String getLogicPath() {
    return this.logicPath;
  }

  public void setRealPath(String realPath) {
    this.realPath=realPath;
  }

  public String getRealPath() {
    return this.realPath;
  }

  public void setType(Byte type) {
    this.type=type;
  }

  public Byte getType() {
    return this.type;
  }

  public void setNamenodeAddr(String namenodeAddr) {
    this.namenodeAddr=namenodeAddr;
  }

  public String getNamenodeAddr() {
    return this.namenodeAddr;
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

  public Byte getShareType() {
    return shareType;
  }

  public void setShareType(Byte shareType) {
    this.shareType = shareType;
  }

  public String getClusterInfos() {
    return clusterInfos;
  }

  public void setClusterInfos(String clusterInfos) {
    this.clusterInfos = clusterInfos;
  }

  public String getClusterId() {
    return clusterId;
  }

  public void setClusterId(String clusterId) {
    this.clusterId = clusterId;
  }

  @Override
  public String toString() {
    return "DirInfoDto{" +
            "dirId=" + dirId +
            ", appId=" + appId +
            ", logicPath='" + logicPath + '\'' +
            ", realPath='" + realPath + '\'' +
            ", type=" + type +
            ", shareType=" + shareType +
            ", namenodeAddr='" + namenodeAddr + '\'' +
            ", createTime=" + createTime +
            ", updateTime=" + updateTime +
            ", state=" + state +
            ", clusterInfos='" + clusterInfos + '\'' +
            ", clusterId='" + clusterId + '\'' +
            '}';
  }
}
