package com.tencent.mdfs.provider.pojo;

import java.io.Serializable;

public class NameNodeIp implements Serializable {
    private long id;
    private String nnIp;
    private String clusterId;
    private String availableNn;

    public NameNodeIp(String clusterId, String availableNn) {
        this.clusterId = clusterId;
        this.availableNn = availableNn;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getClusterId() {
        return clusterId;
    }

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    public String getAvailableNn() {
        return availableNn;
    }

    public void setAvailableNn(String availableNn) {
        this.availableNn = availableNn;
    }

    public String getNnIp() {
        return nnIp;
    }

    public void setNnIp(String nnIp) {
        this.nnIp = nnIp;
    }
}
