package com.tencent.mdfs.config;

/**
 * create:lichunxiao
 * Date:2019-03-14
 */
public class Config {

    public static final String KEY_APP_ID = "app_id";
    public static final String KEY_JOB_ID = "job_id";

    public static final String CONFIG_AUDIT_LOG_ON = "config.audit.log.on";
    public static final String CONFIG_AUDIT_LOG_LOCAL_ON = "config.audit.log.local.on";
    public static final String CONFIG_DEBUG_LOG_ON = "config.debug.log.on";
    public static final String CONFIG_LOG_LEVEL = "config.log.level";
    public static final String CONFIG_TDC_SHARE_MODE = "config.tdc.share.mode";
    public static final String CONFIG_MAX_LEVEL = "config.max.level";

    public static final String CONFIG_METRIC_VNN_SERVER_MONITOR = "config.metric.vnn.server.monitor";
    public static final String CONFIG_METRIC_VNN_VNN2NN_MONITOR = "config.metric.vnn.vnn2nn.monitor";
    public static final String CONFIG_METRIC_VNN_CONNPOOL_MONITOR = "config.metric.vnn.connpool.monitor";
    public static final String CONFIG_METRIC_VNN_SERVER_MONITOR_DEFAULT = "metric_vnn_server_server";
    public static final String CONFIG_METRIC_VNN_CONNPOOL_MONITOR_DEFAULT = "metric_vnn_server_connpools";
    public static final String CONFIG_METRIC_VNN_VNN2NN_MONITOR_DEFAULT = "metric_vnn_server_vnn2nn";


    public static final String CONFIG_KEY_DB_REFRESH_PERIOD = "config.key.db.refresh.period";
    public static final int CONFIG_KEY_DB_REFRESH_PERIOD_DEFAULT = 60;

    public static final boolean MIG_DFS_CLIENT_NEED_REFUSE_HDFS_DEFAULT = false;


    public static final String MIG_DFS_CLIENT_NEED_REFUSE_HDFS_KEY = "pcg.dfs.client.need.refuse.hdfs";
    public static final String MIG_DFS_CLIENT_NEED_CHECK_PERMISSION_KEY = "pcg.dfs.client.need.check.permission";
    public static final String PCG_DFS_VNN_SALT = "pcg.vnn.client.salt";
    public static final String MIG_DFS_CLIENT_NEED_REFUSE_ADMIN_OPERATOR_KEY = "pcg.dfs.client.need.refuse.adminoperator";

    public static String PCG_DFS_CLIENT_NEED_CHECK_PLATFORM_TOKEN = "pcg.dfs.client.need.check.platform.token";
}
