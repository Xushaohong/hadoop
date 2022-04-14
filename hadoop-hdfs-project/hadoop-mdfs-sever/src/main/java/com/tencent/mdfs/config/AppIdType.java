package com.tencent.mdfs.config;

public enum AppIdType {
    ADMIN(1,"admin"),
    DIRROOT(2,"dirRoot");

    private int value;
    private String desc;
    AppIdType(int value,String desc){
        this.value = value;
        this.desc = desc;
    }
    public int value(){
        return this.value;
    }


}
