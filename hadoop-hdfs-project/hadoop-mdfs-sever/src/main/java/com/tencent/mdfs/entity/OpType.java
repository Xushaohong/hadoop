package com.tencent.mdfs.entity;

/**
 * create:chunxiaoli
 * Date:2/6/18
 */
public enum OpType {

    READ(1,"read"),
    WRITE(2,"write");

    private int value;
    private String desc;

    OpType(int value,String desc){
        this.value=value;
        this.desc=desc;
    }

    public int getValue() {
        return value;
    }
}
