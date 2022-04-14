package com.tencent.mdfs.nnproxy.context;

import java.util.HashMap;
import java.util.Map;

public class RpcContextUtils {

    public static final ThreadLocal<Map<String, String>> REQ_CONTEXT_THREADLOCAL = new ThreadLocal<Map<String, String>>() {
        @Override
        protected Map<String, String> initialValue() {
            return new HashMap<>();
        }
    };

    public static void setRealNNIp(String addr) {
        REQ_CONTEXT_THREADLOCAL.get().put("callee", addr);
    }

    public static String getRealNNIp() {
        return REQ_CONTEXT_THREADLOCAL.get().get("callee");
    }
}
