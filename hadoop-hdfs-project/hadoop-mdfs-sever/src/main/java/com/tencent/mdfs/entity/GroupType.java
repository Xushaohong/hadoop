package com.tencent.mdfs.entity;

/**
 * create:chunxiaoli
 * Date:2/6/18
 */
public enum GroupType {

    NORMAL(1,"normal"),
    VIRTUAL(2,"virtual"),
    PERSONAL(3,"personal");

    private int value;
    private String desc;

    GroupType(int value,String desc){
        this.value=value;
        this.desc=desc;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }
}
