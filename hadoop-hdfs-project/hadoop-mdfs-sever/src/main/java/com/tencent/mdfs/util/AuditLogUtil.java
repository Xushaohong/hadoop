package com.tencent.mdfs.util;


import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;


/**
 * create:lichunxiao
 * Date:2019-03-14
 */
public class AuditLogUtil {

    public final static String CMD_CREATE = "create";
    public final static String CMD_RENAME = "create";
    public final static String CMD_MKDIRS = "mkdirs";
    public final static String CMD_APPEND = "append";

    private static final Logger logger = LogManager.getLogger("vnnaudit");

    private static final ThreadPoolExecutor pool = new ThreadPoolExecutor(5,
            5,
            0L, TimeUnit.MILLISECONDS, new LinkedBlockingDeque<Runnable>(100000),
            new ThreadFactory() {
                private final AtomicInteger threadNumber = new AtomicInteger(1);

                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r, "audit-log" + threadNumber.getAndIncrement());
                    t.setDaemon(true);
                    return t;
                }
            }, new RejectedExecutionHandler() {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            logger.error("too much audit log ");
        }
    });


    public static void log(String s) {
        logger.info(s);
    }

    public static void audit(final int status, final String cmd, final long startTime) {

        /*try {


            final String taskType = ContextUtil.getContextByKey(TASK_TYPE);
            final String attemptId = ContextUtil.getContextByKey(ATTEMPTID);
            final String taskIp = ContextUtil.getContextByKey(TASK_IP);
            final String taskId = ContextUtil.getContextByKey(TASK_ID);

            final String ip = ContextUtil.getClientIp();
            final String caller = ContextUtil.getCaller();
            final Map<String, Object> map = ContextUtil.getJobInfoMap();
            final String jobId = (String) map.get(Config.KEY_JOB_ID);
            final Integer appId = (Integer) map.get(Config.KEY_APP_ID);
            final String applicationId = ContextUtil.getContextByKey(APPLICATIONID);
            final String src = ThreadLocalUtil.src.get();
            final String dst = ThreadLocalUtil.dst.get();
            final RouteInfo routeInfo = ThreadLocalUtil.routeInfo.get();

            final long currentTime = (System.currentTimeMillis());

            final long time = (currentTime - startTime);

            pool.submit(new Runnable() {
                @Override
                public void run() {


                    try {

                        final String schema = VnnPathUtil.isMDFS(src) ? "1" : "0";

                        StringBuilder sb = new StringBuilder();

                        sb.append(status).append("|")
                                .append(schema).append("|")
                                .append(ip).append("|")
                                .append((routeInfo != null ? routeInfo.addr : null)).append("|")
                                .append(cmd).append("|")
                                .append(src).append("|")
                                .append(applicationId).append("|")
                                .append(jobId).append("|")
                                .append(appId).append("|")
                                .append(caller).append("|")
                                .append(time);

                        if (VnnConfigUtil.isAuditLogLocalOn()) {
                            logger.info(sb.toString());
                        }


                        sb.append("|").append(currentTime)
                                .append("|").append(taskId)
                                .append("|").append(taskIp)
                                .append("|").append(dst)
                                .append("|").append(taskType);

                        String msg = sb.toString();

                        DMQUtil.sendAuditMsg(msg);

                    } catch (Exception e) {
                        e.printStackTrace();
                        VnnLogUtil.err("do audit error " + e);
                    }


                }
            });
        } catch (Exception e) {
            e.printStackTrace();
            VnnLogUtil.err(" try to log audit error " + e);

        }*/
    }


}
