package com.tencent.mdfs.entity;

import java.util.Set;

/**
 * create:chunxiaoli
 * Date:2/6/18
 */
public class Group {
    private String name;

    private GroupType type;

    private Set<DirInfo> dirs;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public GroupType getType() {
        return type;
    }

    public void setType(GroupType type) {
        this.type = type;
    }

    public Set<DirInfo> getDirs() {
        return dirs;
    }

    public void setDirs(Set<DirInfo> dirs) {
        this.dirs = dirs;
    }

    @Override
    public String toString() {
        return "Group{" +
                "name='" + name + '\'' +
                ", type=" + type +
                ", dirs=" + dirs +
                '}';
    }
}
