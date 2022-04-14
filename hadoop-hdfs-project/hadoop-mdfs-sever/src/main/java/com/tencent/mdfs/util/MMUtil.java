package com.tencent.mdfs.util;

import com.qq.cloud.taf.server.config.ConfigurationManager;
import com.qq.fz.ppreport.PPLogStatApi;
import com.qq.mig.hadoop.metric.client.TafMetricClientManager;
import com.tencent.mdfs.nnproxy.context.ContextUtil;
import com.tencent.mdfs.nnproxy.context.RpcContextUtils;
import java.util.Arrays;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * create:chunxiaoli
 * Date:5/8/18
 */
public class MMUtil {


    private Logger logger  = LoggerFactory.getLogger( MMUtil.class );

    private static final ThreadPoolExecutor pool = new ThreadPoolExecutor(5,
            5,
            0L, TimeUnit.MILLISECONDS, new LinkedBlockingDeque<Runnable>(100000),
            new ThreadFactory() {
                private final AtomicInteger threadNumber = new AtomicInteger(1);
                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r, "mm-report-" + threadNumber.getAndIncrement());
                    t.setDaemon(true);
                    return t;
                }
            }, new ThreadPoolExecutor.DiscardPolicy());


    public static void reportVNNToNN(final String method, final int status, final long startTime) {
        report(VnnConfigUtil.getVnn2NNMonitorName(),method,status,startTime);
    }



    public static void reportVnnServer(final String method, final int status, final long startTime) {
        report(VnnConfigUtil.getVNNServerMonitorName(),method,status,startTime);
    }

    private static void report(final String monitorName,final String method,
                               final int status, final long startTime) {
        try {

            final long time=System.currentTimeMillis()-startTime;

            String serverName = "";//serverConfig.getServerName();


            final String[] keys = new String[]{
                    ContextUtil.getRemoteIp(),
                    TafConfigUtil.getCalleeIp(),
                    RpcContextUtils.getRealNNIp(),
                    ContextUtil.getCaller(),
                    TafServerConfigUtil.getSetName(),
                    serverName,
                    ContextUtil.getSchema(),
                    method,

            };


            pool.submit(new Runnable() {
                @Override
                public void run() {

                    float[] values = new float[]{
                            1, status == 0 ? 1 : 0, time
                    };

                    TafMetricClientManager.getInstance().report(monitorName,keys,values);

                    PPLogStatApi.log(monitorName,keys,values);

                    //VnnLogUtil.log("report : "+monitorName+" "+ Arrays.toString(keys) +" "+ Arrays.toString(values));

                }
            });

        } catch (Throwable e) {
            e.printStackTrace();
            VnnLogUtil.err("reportClientNamenodeProtocolServantHandler error " + method + " " + e);
        }
    }


    public static void reportCheckPermissionAndGetRoute(long startTime, int status) {
        //report(VnnConfigUtil.getVnnMonitorName(),"CheckPermissionAndGetRoute",status,startTime);
    }

    public static void reportVNNDataRefresh(final long  dirsSize, final long time,final int status) {


        pool.submit(new Runnable() {
            @Override
            public void run() {

                final String[] keys = new String[]{
                        TafConfigUtil.getCalleeIp(),
                        System.getenv("HOSTNAME")
                };
//                ConfigurationManager.getInstance().getserverConfig().getServerName()

                float values[] = new float[]{
                        1,status==0?1:0,time,dirsSize
                };

                VnnLogUtil.log(VnnConfigUtil.getVnnRefreshMonitorName()+"  "+ Arrays.toString(keys) +" "+ Arrays.toString(values));

                PPLogStatApi.log(VnnConfigUtil.getVnnRefreshMonitorName(),keys,values);
            }
        });
    }

    public static void sendDatamapSendRst(final String app, final String server, final String file,
                                          final int sendSize, final int sendRst, final long startTime ){
        pool.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    final String[] keys = new String[]{
                            app,
                            server,
                            file,
                            ConfigurationManager.getInstance().getserverConfig().getLocalIP(),
                            ConfigurationManager.getInstance().getserverConfig().getServerName(),
                            ConfigurationManager.getInstance().getserverConfig().getCommunicatorConfig().getSetDivision(),
                            sendRst + ""
                    };
                    final float[] values = new float[]{
                            sendSize,
                            1,
                            System.currentTimeMillis() - startTime
                    };
                    PPLogStatApi.log(VnnConfigUtil.getDatamapSendRst(), keys, values);
                }catch (Exception e){
                    VnnLogUtil.err("sendDatamapSendRst failed"+e);
                }
            }
        });
    }


    public static void sendDatamapReject() {
        //metric_rejected_msg
        pool.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    final String[] keys = new String[]{
                            ConfigurationManager.getInstance().getserverConfig().getLocalIP(),
                            ConfigurationManager.getInstance().getserverConfig().getServerName(),
                            ConfigurationManager.getInstance().getserverConfig().getCommunicatorConfig().getSetDivision()
                    };
                    final float[] values = new float[]{
                            1
                    };
                    VnnLogUtil.log(VnnConfigUtil.getDatamapRejected() + "  " + Arrays.toString(keys) + " " + Arrays.toString(values));

                    PPLogStatApi.log(VnnConfigUtil.getDatamapRejected(), keys, values);
                }catch (Exception e){
                    VnnLogUtil.err("sendDatamapReject failed"+e);
                }
            }
        });
    }

    public static void reportInvalidDelete(final String method) {


        pool.submit(new Runnable() {
            @Override
            public void run() {

                final String[] keys = new String[]{
                        ContextUtil.getRemoteIp(),
                        ContextUtil.getCaller(),
                        TafConfigUtil.getCalleeIp(),
                        TafServerConfigUtil.getSetName(),

                        ContextUtil.getSchema(),
                        method
                };

                final float[] values =new float[]{
                       1.0f
                };

                PPLogStatApi.log("vnn_metric_invalid_delete",keys,values);
            }
        });
    }
}
