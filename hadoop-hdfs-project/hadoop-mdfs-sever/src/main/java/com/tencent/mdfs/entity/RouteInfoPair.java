package com.tencent.mdfs.entity;

/**
 * create:chunxiaoli
 * Date:6/5/18
 */
public class RouteInfoPair {

    public RouteInfoPair(){

    }

    public  String realPath;
    public  String addr;

    public String getRealPath() {
        return realPath;
    }

    public void setRealPath(String realPath) {
        this.realPath = realPath;
    }

    public String getAddr() {
        return addr;
    }

    public void setAddr(String addr) {
        this.addr = addr;
    }

    @Override
    public String toString() {
        return "RouteInfoPair{" +
                "realPath='" + realPath + '\'' +
                ", addr='" + addr + '\'' +
                '}';
    }
}
