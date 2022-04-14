package com.tencent.mdfs.storage;


import com.tencent.mdfs.entity.RouteInfo;

/**
 * create:chunxiaoli
 * Date:4/8/18
 */
public interface VnnConfigService {
    /**
     * 根据路径名返回文件路径信息
     * @param path
     * @return
     */
    RouteInfo getRouteInfo(String path);
}
