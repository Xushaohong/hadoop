package com.tencent.mdfs.provider.pojo;

import java.util.Date;

/**
 */
public class AppDirRelation {
    /**
    * id
    */
    private Integer id;

    /**
    * app_id
    */
    private Long appId;

    /**
    * dir_id
    */
    private Long dirId;

    /**
    * 权限类型:[1:只读 2:读写]
    */
    private Byte type;

    private Byte onlineType;
    /**
    * 创建时间
    */
    private Date createTime;

    /**
    * 更新时间
    */
    private Date updateTime;

    public Byte getOnlineType() {
        return onlineType;
    }

    public void setOnlineType(Byte onlineType) {
        this.onlineType = onlineType;
    }

    /**
    * 逻辑删除标记[1:正常 2:删除]
    */
    private Byte state;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Long getAppId() {
        return appId;
    }

    public void setAppId(Long appId) {
        this.appId = appId;
    }

    public Long getDirId() {
        return dirId;
    }

    public void setDirId(Long dirId) {
        this.dirId = dirId;
    }

    public Byte getType() {
        return type;
    }

    public void setType(Byte type) {
        this.type = type;
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

    @Override
    public String toString() {
        return "AppDirRelation{" +
                "id=" + id +
                ", appId=" + appId +
                ", dirId=" + dirId +
                ", type=" + type +
                ", onlineType=" + onlineType +
                ", createTime=" + createTime +
                ", updateTime=" + updateTime +
                ", state=" + state +
                '}';
    }
}