package com.tencent.mdfs.entity;

import java.util.List;

/**
 * create:chunxiaoli
 * Date:2/6/18
 */
public class FTItem {

    private String name;

    private List<Group> groups;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Group> getGroups() {
        return groups;
    }

    public void setGroups(List<Group> groups) {
        this.groups = groups;
    }

    @Override
    public String toString() {
        return "FTItem{" +
                "name='" + name + '\'' +
                ", groups=" + groups +
                '}';
    }
}
