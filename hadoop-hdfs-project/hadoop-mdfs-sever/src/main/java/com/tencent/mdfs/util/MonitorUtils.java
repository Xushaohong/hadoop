package com.tencent.mdfs.util;

import com.tencent.teg.monitor.pojo.CurveCalMethod;
import com.tencent.teg.monitor.pojo.SDKCurveReport;
import com.tencent.teg.monitor.sdk.TegMonitor;
import com.tencent.teg.monitor.utils.Pair;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Need to configure environment variables：
 * TEG_METRICS_APPID, zhiyan.oa.com monitor appId
 * VNN_DOMAIN：vnn domain
 */
public class MonitorUtils {

    private static final Logger log = LogManager.getLogger(MonitorUtils.class);

    private static String LOCAL_IP = TafConfigUtil.getCalleeIp();


    static {
        initTegMonitor();
    }

    static void initTegMonitor() {
        log.info("teg init monitor");
        TegMonitor.init();
    }

    public static Map<String, String> getCommonKeys() {
        String hostName = System.getenv("HOSTNAME");
        String ip = TafConfigUtil.getCalleeIp();
        String domain =  System.getenv("VNN_DOMAIN");
        if (StringUtils.isBlank(domain)) {
            domain = "default";
        }
        Map<String, String> commKeys = new LinkedHashMap<>();
        commKeys.put("hostName", hostName);
        commKeys.put("ip", ip);
        commKeys.put("domain", domain);
        return commKeys;
    }


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



    public static void reportVnnServer(final Map<String, String> keys, final boolean status, final long startTime) {
        //report(VnnConfigUtil.getVNNServerMonitorName(), keys, status,startTime);
        reportTeg(VnnConfigUtil.getVNNServerMonitorName(), keys, status,startTime);
    }

    public static void reportConnectionManagerPools(final int numActiveConnections,
            final int numConnectionPools, final int numConnections, final int numCreatingConnections) {
        try {
            final String monitorName = VnnConfigUtil.getConnPoolMonitorName();
            pool.submit(new Runnable() {
                @Override
                public void run() {
                    SDKCurveReport curveReport = new SDKCurveReport();
                    curveReport.setAppMark(monitorName);
                    curveReport.setInstanceMark(LOCAL_IP);
                    curveReport.setTagMap(getCommonKeys());

                    Map<String, Pair<CurveCalMethod, Double>> metricVal = new HashMap<>();
                    metricVal.put("nums", new Pair<>(CurveCalMethod.ADD, 1.0));
                    metricVal.put("numActiveConnections", new Pair<>(CurveCalMethod.MAX, numActiveConnections * 1.0));
                    metricVal.put("numConnectionPools", new Pair<>(CurveCalMethod.MAX, numConnectionPools * 1.0));
                    metricVal.put("numConnections", new Pair<>(CurveCalMethod.MAX, numConnections * 1.0));
                    metricVal.put("numCreatingConnections", new Pair<>(CurveCalMethod.MAX, numCreatingConnections * 1.0));
                    curveReport.setMetricVal(metricVal);
                    int ret = TegMonitor.addCurveData(curveReport);
                    if (ret != 0) {
                        log.error("teg report ret={}!", ret);
                    }

                }
            });

        } catch (Throwable e) {
            e.printStackTrace();
            VnnLogUtil.err("reportClientNamenodeProtocolServantHandler error numActiveConnections" + numActiveConnections + " " + e);
        }
    }

    private static void reportTeg(final String monitorName, final Map<String, String> keys, final boolean status, final long startTime) {
        try {
            final long time=System.currentTimeMillis() - startTime;
            pool.submit(new Runnable() {
                @Override
                public void run() {
                    SDKCurveReport curveReport = new SDKCurveReport();
                    curveReport.setAppMark(monitorName);
                    curveReport.setInstanceMark(LOCAL_IP);
                    curveReport.setTagMap(keys);

                    Map<String, Pair<CurveCalMethod, Double>> metricVal = new HashMap<>();
                    metricVal.put("totalNum", new Pair<>(CurveCalMethod.ADD, 1.0));
                    metricVal.put("successNum", new Pair<>(CurveCalMethod.ADD, status ? 1.0 : 0.0));
                    metricVal.put("eltTimeAvg", new Pair<>(CurveCalMethod.AVG, time*1.0));
                    metricVal.put("eltTimeMid", new Pair<>(CurveCalMethod.MIDDLE, time*1.0));
                    metricVal.put("eltTimeMin", new Pair<>(CurveCalMethod.MIN, time*1.0));
                    metricVal.put("eltTimeMax", new Pair<>(CurveCalMethod.MAX, time*1.0));
                    curveReport.setMetricVal(metricVal);
                    int ret = TegMonitor.addCurveData(curveReport);
                    if (ret != 0) {
                        log.error("teg report ret={}!", ret);
                    }

                }
            });

        } catch (Throwable e) {
            e.printStackTrace();
            VnnLogUtil.err("reportClientNamenodeProtocolServantHandler error " + keys + " " + e);
        }
    }

}
