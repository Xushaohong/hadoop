package com.tencent.mdfs.util;

import com.qq.cloud.router.client.LBGetType;
import com.qq.cloud.router.client.LBType;
import com.qq.cloud.router.client.Result;
import com.qq.cloud.router.client.Router;
import com.qq.cloud.router.client.RouterConfig;
import com.qq.cloud.router.client.RouterFactory;
import com.qq.cloud.router.client.RouterRequest;
import com.qq.cloud.router.client.RouterResponse;
import com.qq.mig.hadoop.metric.util.LogUtil;

public class SeerUtil {
    private static final String mainObj = "taf.tafregistry.QueryObj";

    public static String getLocator() {


        String loator = null;

        try{
            RouterConfig.setSeerApiKey("hadoop");  // 全局设置API KEY

            // Agent方式

            RouterConfig config = new RouterConfig();
            //MetricConfigUtil.getProxyConfig().getLocator();
            //String locator = Constants.SEER_CLIENT_LOCATOR_DEFAULT;
            //config.setRouterLocater(locator);
            Router router = RouterFactory.getInstance().createApiRouter(config);

            RouterRequest routerRequest = new RouterRequest();
            routerRequest.setObj( mainObj );
            routerRequest.setLbGetType(LBGetType.LB_GET_ALL);  //选取set方式获取  必填

            //routerRequest.setCallModuleName(“login.TestServer”); //设置主模块名  模调上报时用到，推荐填写，默认为本机IP
            //routerRequest.setSetInfo(“a.a.a”)   //设置set信息，当LbGetType为LBGetType.LB_GET_SET时必填

            routerRequest.setLbType(LBType.LB_TYPE_STATIC_WEIGHT); //使用静态权重负载算法  必填

            //   request.setHashKey(123); // 设置hashkey，当LbType为LBType.LB_TYPE_CST_HASH时必填
            Result<RouterResponse> responseResult = router.getRoute( routerRequest );
            RouterResponse response = responseResult.getData();

            if(response!=null){
                String ip = response.getIP();
                int port = response.getPort();

                if(ip!=null&&!("".equals(ip))){
                    loator =  mainObj+"@tcp -h " + ip + " -p " + port + " -t 10000";
                }

            }
        }catch (Exception e){
            e.printStackTrace();
            LogUtil.error("get locator error "+e);
        }

        return loator;
    }

    public static String getRandomNode(String obj) {
        if (obj == null || obj.isEmpty()) {
            throw new IllegalArgumentException("obj name can not be empty");
        }
        RouterConfig.setSeerApiKey("hadoop");

        RouterConfig config = new RouterConfig();
        Router router = RouterFactory.getInstance().createApiRouter(config);

        RouterRequest routerRequest = new RouterRequest();
        routerRequest.setObj(obj);
        routerRequest.setLbGetType(LBGetType.LB_GET_ALL);
        routerRequest.setLbType(LBType.LB_TYPE_RANDOM);

        Result<RouterResponse> responseResult = router.getRoute(routerRequest);
        RouterResponse response = responseResult.getData();

        if (response.getIP() == null) {
            throw new RuntimeException("can not find any node for:" + obj);
        }
        return response.getIP() + ":" + response.getPort();
    }
}
