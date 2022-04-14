package com.tencent.mdfs.provider.pojo;

import java.util.Date;

/**
 */
public class AppInfo {
    /**
    * 记录自增id
    *
    * 应用组id,全局唯一
    */
    private Long appId;

    /**
    * 应用组名称
    */
    private String appName;

    /**
    * 应用组编码，全局唯一
    */
    private String appCode;

    /**
    * 类型[1:普通,2:个人]
    */
    private Byte type;

    /**
    * BG名称
    */
    private String bg;

    /**
    * 产品
    */
    private String product;

    /**
    * 业务集
    */
    private String business;

    /**
    * 负责人(管理员)
    */
    private String owner;

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


    public Long getAppId() {
        return appId;
    }

    public void setAppId(Long appId) {
        this.appId = appId;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getAppCode() {
        return appCode;
    }

    public void setAppCode(String appCode) {
        this.appCode = appCode;
    }

    public Byte getType() {
        return type;
    }

    public void setType(Byte type) {
        this.type = type;
    }

    public String getBg() {
        return bg;
    }

    public void setBg(String bg) {
        this.bg = bg;
    }

    public String getProduct() {
        return product;
    }

    public void setProduct(String product) {
        this.product = product;
    }

    public String getBusiness() {
        return business;
    }

    public void setBusiness(String business) {
        this.business = business;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
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
}