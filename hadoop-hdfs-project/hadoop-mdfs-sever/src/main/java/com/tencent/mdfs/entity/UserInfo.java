package com.tencent.mdfs.entity;

import java.util.Set;

/**
 * create:chunxiaoli
 * Date:2/6/18
 */
public class UserInfo {

    //rtx name
    private String name;

    //ft list
    private Set<String> ftInfoSet;

    private int type;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Set<String> getFtInfoSet() {
        return ftInfoSet;
    }

    public void setFtInfoSet(Set<String> ftInfoSet) {
        this.ftInfoSet = ftInfoSet;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }
}
