package com.tencent.mdfs.provider.pojo;

import java.io.Serializable;
import java.util.Date;

public class AppRelation implements Serializable {
    private long id;
    private long parent;
    private long son;
    private Date createTime;
    private Date updateTime;
    private int state;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getParent() {
        return parent;
    }

    public void setParent(long parent) {
        this.parent = parent;
    }

    public long getSon() {
        return son;
    }

    public void setSon(long son) {
        this.son = son;
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

    public int getState() {
        return state;
    }

    public void setState(int state) {
        this.state = state;
    }
}
