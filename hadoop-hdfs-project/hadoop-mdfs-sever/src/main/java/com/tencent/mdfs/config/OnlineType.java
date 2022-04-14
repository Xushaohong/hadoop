package com.tencent.mdfs.config;

public enum OnlineType {
    GRAY((byte)1,"gray"),
    ONLINE((byte)2,"real online");

    private byte value;
    private String desc;
    OnlineType(byte value,String desc){
        this.value = value;
        this.desc = desc;
    }
    public byte value(){
        return this.value;
    }
}
