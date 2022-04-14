package com.tencent.mdfs.config;

/**
 * create:chunxiaoli
 * Date:2/6/18
 */
public class MigDFSConfigKeys {

    public static final String DFS_CLIENT_USE_MIG_CLIENT_PROTOCOL_KEY = "dfs.client.mig.use.mig.client.protocol";

    public static final boolean DFS_CLIENT_USE_MIG_CLIENT_PROTOCOL_DEFAULT = false;

    public static final String MIG_DFS_CLIENT_NNPROXY_OBJ_KEY = "mig.dfs.client.nnproxy.objName";

    public static final String MIG_DFS_CLIENT_NNPROXY_SET_KEY = "mig.dfs.client.nnproxy.set";

    public static final String MIG_DFS_CLIENT_NNPROXY_CONFIG_OBJ_KEY = "mig.dfs.client.nnproxy.config.objName";

    public static final String MIG_DFS_CLIENT_NNPROXY_CONFIG_OBJ_DEFAULT
            = "NameNodeProxy.nnproxy.clientToVnnConfig";

    public static final String MIG_DFS_CLIENT_NNPROXY_OBJ_DEFAULT
            = "NameNodeProxy.nnproxy.nnproxy";

    public static final String MIG_DFS_CLIENT_NNPROXY_SET_DEFAULT
            = "";


    public static final String MIG_DFS_CLIENT_NNPROXY_TEST_OBJ_DEFAULT
            = "NameNodeProxy.vnntest.nnproxy";

    public static final String MIG_DFS_CLIENT_NNPROXY_TEST_CONFIG_OBJ_DEFAULT
            = "NameNodeProxy.vnntest.clientToVnnConfig";



    public static final String MIG_KEY_CLIENT_NAME_NODE_URI="mig.key.client.namenode.uri";
    public static final String MIG_KEY_CLIENT_USER="mig.key.client.user";
    public static final String MIG_KEY_CONFIG_CLIENT_USER = "key.config.client.user";
    public static final String MIG_KEY_APP_NAME = "mig.key.app.name";

    public static final String DFS_CLIENT_DIRECTOR_ROUTE = "dfs.client.mig.use.mig.client.protocol";
    public static final boolean DFS_CLIENT_DIRECTOR_ROUTE_DEFAULT = false;

    public static final String DFS_CLIENT_SCHEMA = "schme";

    public static final String MIG_DFS_CLIENT_NEED_CHECK_PERMISSION_KEY = "mig.dfs.client.need.check.permission";
    public static final boolean MIG_DFS_CLIENT_NEED_CHECK_PERMISSION_DEFAULT = false;

    public static final String MIG_DFS_PERMISSION_CHECK_POLICY = "mig.dfs.client.permission.check.policy";

    public static final String DFS_CLIENT_MIG_MAX_DIR_LEVEL_LENGTH_KEY = "dfs.client.mig.max.dir.level.length";
    public static final int DFS_CLIENT_MIG_MAX_DIR_LEVEL_LENGTH_DEFAULT = 10;

    public static final String DFS_CLIENT_MIG_MAX_DIR_CHECK_LEVEL_KEY = "dfs.client.mig.max.dir.check.level";
    public static final int DFS_CLIENT_MIG_MAX_DIR_CHECK_LEVEL_DEFAULT = 10;


    public static final String NNPROXY_USER_TDC_SHARE_MODE_KEY = "nnproxy.use.tdc.share.mode";

    public static final boolean NNPROXY_USER_TDC_SHARE_MODE_DEFAULT = false;

    public static final String NNPROXY_BLACK_LIST_ON_KEY = "nnproxy.blacklist.on";
    public static final String NNPROXY_WHITE_LIST_ON_KEY = "nnproxy.whitelist.on";

    public static final boolean NNPROXY_BLACK_LIST_ON_DEFAULT = false;
    public static final boolean NNPROXY_WHITE_LIST_ON_DEFAULT = false;

    public static final String MIG_KEY_VNN_DEFAULT_NAME_NODE = "mig.vnn.default.namenode";


    public static final String METRIC_CLIENT_SERVICE_NAME_KEY = "metric_client_service_name";

    public static final String FZ_LOG_APP = "fz_log_app";
    public static final String FZ_LOG_APP_DEFAULT = "NameNodeProxy";
    public static final String FZ_LOG_SERVER = "fz_log_server";
    public static final String FZ_LOG_SERVER_DEFAULT = "vnntest";
    public static final String FZ_LOG_FILE ="fz_log_file";
    public static final String FZ_LOG_FILE_DEFAULT = "datamap";

}
