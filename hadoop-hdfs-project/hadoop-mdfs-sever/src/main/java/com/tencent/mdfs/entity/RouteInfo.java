package com.tencent.mdfs.entity;

import java.util.List;

/**
 * create:chunxiaoli
 * Date:2/7/18
 */
public class RouteInfo {

    public Long id;

    public final String originPath;

    public final String logicPath;

    public final String realPath;

    public final String finalPath;

    //namenode ip:port
    public String addr;

    private String clusterId;

    private String originLogicPath;
    //0 hdfs  1.完全匹配 2.子目录匹配 3.默认nn
    private  int matchType;

    public static final int MATCH_TYPE_HDFS=0;
    public static final int MATCH_TYPE_COMPLETE=1;
    public static final int MATCH_TYPE_SUBDIR=2;
    public static final int MATCH_TYPE_DEFAULT_NN=3;



    private List<RouteInfoPair> clusterInfos;

    public String getAddr() {
        return addr;
    }

    public void setAddr(String addr) {
        this.addr = addr;
    }

    public RouteInfo(final String logicPath,
                     final String realPath,
                     final String addr,
                     final String originPath,
                     final String finalPath){
        this.logicPath=logicPath;
        this.addr=addr;
        this.realPath=realPath;
        this.finalPath=finalPath;
        this.originPath=originPath;
    }

    public RouteInfo(
                     final long id,
                     final String logicPath,
                     final String realPath,
                     final String addr,
                     final String originPath,
                     final String finalPath){
        this.id = id;
        this.logicPath=logicPath;
        this.addr=addr;
        this.realPath=realPath;
        this.finalPath=finalPath;
        this.originPath=originPath;
    }

    public RouteInfo(
                     final Long id,
                     final String logicPath,
                     final String realPath,
                     final String addr,
                     final String originPath,
                     final String finalPath,
                     final String clusterId
    ){
        this.id = id;
        this.logicPath=logicPath;
        this.clusterId=clusterId;
        this.addr = addr;
        this.realPath=realPath;
        this.finalPath=finalPath;
        this.originPath=originPath;
    }



    /*public RouteInfo(RouteInfo ret) {
        this.realPath=ret.realPath;
        this.logicPath=ret.logicPath;
        this.addr=ret.addr;
    }*/


    @Override
    public String toString() {
        return "RouteInfo{" +
                "id=" + id +
                ", originPath='" + originPath + '\'' +
                ", logicPath='" + logicPath + '\'' +
                ", realPath='" + realPath + '\'' +
                ", finalPath='" + finalPath + '\'' +
                ", addr='" + addr + '\'' +
                ", clusterId='" + clusterId + '\'' +
                ", originLogicPath='" + originLogicPath + '\'' +
                ", matchType=" + matchType +
                ", clusterInfos=" + clusterInfos +
                '}';
    }

    public String getClusterId() {
        return clusterId;
    }

    public int getMatchType() {
        return matchType;
    }

    public void setMatchType(int matchType) {
        this.matchType = matchType;
    }

    public List<RouteInfoPair> getClusterInfos() {
        return clusterInfos;
    }

    public void setClusterInfos(List<RouteInfoPair> clusterInfos) {
        this.clusterInfos = clusterInfos;
    }

    public String getOriginPath() {
        return originPath;
    }

    public String getOriginLogicPath() {
        return originLogicPath;
    }

    public void setOriginLogicPath(String originLogicPath) {
        this.originLogicPath = originLogicPath;
    }
}
