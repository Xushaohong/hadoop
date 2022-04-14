package com.tencent.mdfs.provider.pojo;

import java.util.Date;

/**
 */
public class DirInfo {
    /**
    * 记录自增id
    *
    * 目录id
    */
    private Long dirId;

    /**
    * 属主应用组id
    */
    private Integer appId;

    /**
    * 逻辑路径  /mtt/profile/hive
    */
    private String logicPath;

    /**
    * 物理路径 /user/mqq/mtt/profile/hive
    */
    private String realPath;

    /**
    * dirType[1:ft 2:personal]
    */
    private Byte type;

    /**
     * 共享标记[1:共享 2:私有]
     */
    private Byte shareType;

    /**
    * namenode地址,ip:port(10.191.128.11:9000|域名)
    */
    private String namenodeAddr;

    /**
    * 创建时间
    */
    private Date createTime;

    /**
    * 更新时间
    */
    private Date updateTime;

    /**
    * 逻辑删除标记[1:正常 2:删除]
    */
    private Byte state;

    /**
     * 跨集群地址
     */
    private String clusterInfos;

    /**
     * 集群名
     */
    private String clusterId;


    public String getClusterId() {
        return clusterId;
    }

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    public Long getDirId() {
        return dirId;
    }

    public void setDirId(Long dirId) {
        this.dirId = dirId;
    }

    public Integer getAppId() {
        return appId;
    }

    public void setAppId(Integer appId) {
        this.appId = appId;
    }

    public String getLogicPath() {
        return logicPath;
    }

    public void setLogicPath(String logicPath) {
        this.logicPath = logicPath;
    }

    public String getRealPath() {
        return realPath;
    }

    public void setRealPath(String realPath) {
        this.realPath = realPath;
    }

    public Byte getType() {
        return type;
    }

    public void setType(Byte type) {
        this.type = type;
    }

    public String getNamenodeAddr() {
        return namenodeAddr;
    }

    public void setNamenodeAddr(String namenodeAddr) {
        this.namenodeAddr = namenodeAddr;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    public Byte getState() {
        return state;
    }

    public void setState(Byte state) {
        this.state = state;
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

    @Override
    public String toString() {
        return "DirInfo{" +
                "dirId=" + dirId +
                ", appId='" + appId + '\'' +
                ", logicPath='" + logicPath + '\'' +
                ", realPath='" + realPath + '\'' +
                ", type=" + type +
                ", shareType=" + shareType +
                ", namenodeAddr='" + namenodeAddr + '\'' +
                ", clusterInfos='" + clusterInfos + '\'' +
                ", createTime=" + createTime +
                ", updateTime=" + updateTime +
                ", state=" + state +
                '}';
    }
}