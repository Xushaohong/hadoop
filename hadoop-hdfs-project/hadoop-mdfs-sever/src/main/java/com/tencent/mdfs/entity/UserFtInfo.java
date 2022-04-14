package com.tencent.mdfs.entity;

import java.util.Set;

/**
 * create:chunxiaoli
 * Date:2/6/18
 */
public class UserFtInfo {
    private String name;
    private Set<FTItem> ftInfoSet;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Set<FTItem> getFtInfoSet() {
        return ftInfoSet;
    }

    public void setFtInfoSet(Set<FTItem> ftInfoSet) {
        this.ftInfoSet = ftInfoSet;
    }

    @Override
    public String toString() {
        return "UserFtInfo{" +
                "name='" + name + '\'' +
                ", ftInfoSet=" + ftInfoSet +
                '}';
    }
}
