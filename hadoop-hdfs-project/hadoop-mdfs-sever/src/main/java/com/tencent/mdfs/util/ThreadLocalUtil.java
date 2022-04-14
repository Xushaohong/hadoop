package com.tencent.mdfs.util;

import com.tencent.mdfs.entity.RouteInfo;

public class ThreadLocalUtil {
    public static ThreadLocal<String> src = new ThreadLocal<>();
    public static ThreadLocal<String> dst = new ThreadLocal<>();

    public static ThreadLocal<RouteInfo> routeInfo = new ThreadLocal<>();
}
