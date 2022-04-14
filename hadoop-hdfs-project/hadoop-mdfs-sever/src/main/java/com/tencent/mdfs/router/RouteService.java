package com.tencent.mdfs.router;


import com.tencent.mdfs.entity.RouteInfo;

/**
 * create:chunxiaoli
 * Date:2/7/18
 */
public interface RouteService {
    RouteInfo route(String logicPath);
}
