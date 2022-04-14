package com.tencent.mdfs.config;

/**
 * create:chunxiaoli
 * Date:2/6/18
 */
public class Constants{


    public static final String WHITE_LIST_DATA_NAME="white_list_data.txt";
    public static final String BLACK_LIST_DATA_NAME="black_list_data.txt";

    public static final String CORE_SITE_NAME="core-site.xml";
    public static final String HDFS_SITE_NAME="hdfs-site.xml";
    public static final String NNPROXY_NAME="nnproxy.xml";

    public static final String HDFS_DEFAULT_PATH="hdfs://cluster1";

    public static final String KEY_JOB_INFO = "custom_job_info";



    public static final String MIG_CLIENT_USER_DEFAULT = "{\"app_id\":1,\"user\":\"mqq\"}";
    public static final String MIG_CLIENT_JOB_DEFAULT = "{\"app_id\":1,\"user\":\"mqq\"}";

    public static final String MIG_CLIENT_USER_TEST = "chunxiao";

    public static final String MIG_DFS_NN_IP_PORT_ACTIVE = "mig.dfs.nn.ipport.active";

    public static final String MIG_DFS_APP_SERVER = "mig.dfs.application.name";

    public static final String MIG_DFS_OLD_SCHEMA = "hdfs";

    public static final String MIG_DFS_NEW_SCHEMA = "mdfs";

    public static final String DFS_SCHEMA_MDFS = "mdfs://";
    public static final String DFS_SCHEMA_CLOUD_HDFS = "/CloudHdfs";
    public static final String DFS_SCHEMA_HDFS = "hdfs://";

    public static final String DEBUG_KEY_CALLER="caller";

    public static final String DEBUG_KEY_ON="mig.debug.on";

    public static final String CLIENT_TO_VNN_MONITOR_NAME="client.to.vnn.monitor.name";

    public static final String VNN_MONITOR_NAME="vnn.monitor.name";

    public static final String CLIENT_TO_VNN_MONITOR_NAME_DEFAULT="metric_vnn_server";

    public static final String VNN_MONITOR_NAME_DEFAULT="metric_vnn_main";

    public static final String VNN_TO_NN_MONITOR_NAME="vnn.to.nn.monitor.name";

    public static final String VNN_TO_NN_MONITOR_NAME_DEFAULT="metric_vnn_vnn2nn";

    public static final String VNN_CLIENT_RPC_MONITOR_NAME="vnn.client.rpc.monitor.name";

    public static final String RESOLVE_HOST_MONITOR_NAME="resolve.host.monitor.name";
    public static final String RESOLVE_HOST_MONITOR_NAME_DEFAULT="metric_resolve_host";

    public static final String MR_TASK_INIT_MONITOR_NAME="mr.task.init.monitor.name";
    public static final String MR_TASK_INIT_MONITOR_NAME_DEFAULT="metric_mr_job_task_init";

    public static final String VNN_TAF_CLIENT_TIMEOUT_NAME="timeout.vnn.taf.client";
    public static final String VNN_TAF_CLIENT_TIMETAG_NAME="timetag.vnn.taf.client";
    public static final String VNN_TAF_CLIENT_RETRY_COUNT="retry.count.vnn.taf.client";
    public static final int VNN_TAF_CLIENT_TIMEOUT_DEFAULT=10*60*1000;
    public static final int VNN_TAF_CLIENT_RETRY_COUNT_DEFAULT=5;
    public static final int VNN_TAF_CLIENT_TAGTIME_DEFAULT = 1000;

    public static final String CLIENT_RETRY_WAY_TAF_CLIENT = "client.retry.way.taf.client";
    public static final int CLIENT_RETRY_WAY_EXPONENT = 1;
    public static final int DEFAULT_RETRY_WAY = CLIENT_RETRY_WAY_EXPONENT;
    //public static final String VNN_CLIENT_RPC_MONITOR_NAME_DEFAULT="hadoop_metric_vnn_taf_client";
    public static final String VNN_CLIENT_RPC_MONITOR_NAME_DEFAULT="metric_rpc_client2vnn";

    public static final String DB_REFRESH_PERIOD="db.refresh.period";
    public static final int  DB_REFRESH_PERIOD_DEFAULT=60;

    public static final String VNN_CONFIG_REFRESH_PERIOD="vnnconfig.refresh.period";
    public static final int  VNN_CONFIG_REFRESH_PERIOD_DEFAULT=60;


    public static final String DATA_MAP_FZLOG_APP_KEY="data_map_fzlog_app_key";
    public static final String DATA_MAP_FZLOG_APP_DEFAULT="NameNodeProxy";

    public static final String DATA_MAP_FZLOG_SERVER_KEY="data_map_fzlog_app_key";
    public static final String DATA_MAP_FZLOG_SERVER_DEFAULT="nnproxy";

    public static final String DATA_MAP_FZLOG_FILE_KEY="data_map_fzlog_app_key";
    public static final String DATA_MAP_FZLOG_FILE_DEFAULT="datamap";

    public static final String DATA_MAP_DMQ_APP_KEY="data_map_fzlog_app_key";
    public static final String DATA_MAP_DMQ_APP_DEFAULT="NameNodeProxy";

    public static final String DATA_MAP_DMQ_SERVER_KEY="data_map_fzlog_server_key";
    public static final String DATA_MAP_DMQ_SERVER_DEFAULT="nnproxy";

    public static final String DATA_MAP_DMQ_FILE_KEY="data_map_fzlog_file_key";
    public static final String DATA_MAP_DMQ_FILE_DEFAULT="datamapdmq";

    public static final String AUDIT_DMQ_APP_KEY="audit_fzlog_app_key";
    public static final String AUDIT_DMQ_APP_DEFAULT="hadoopvnn";

    public static final String AUDIT_DMQ_SERVER_KEY="audit_fzlog_server_key";
    public static final String AUDIT_DMQ_SERVER_DEFAULT="NameNodeProxy";

    public static final String AUDIT_DMQ_FILE_KEY="audit_fzlog_file_key";
    public static final String AUDIT_DMQ_FILE_DEFAULT="audit";

    public static final String DATA_MAP_DMQ_OBJ_NAME_KEY="data_map_dmq_obj_name_key";
    public static final String DATA_MAP_DMQ_OBJ_NAME_DEFAULT="DMQKafkaProxy.DMQKProxyServer4Sz1.ProxyObj";


    public static final String[] DIR_WHITE_LIST=new String[]{
                "/tmp/hive","/tmp"
    };

    public static final int MAX_DIR_LEVEL_LENGTH=3;

    //public static final String YARN_APPLICATION_ID = "mig_yarn_application_id";
    public static final String YARN_APPLICATION_ID = "mig_yarn_application_id";

    public static final String YARN_APPLICATION_TYP_MR = "MAPREDUCE";
    public static final String YARN_APPLICATION_TYP_SPARK = "SPARK";

    public static final String JOB_SUBMIT_IP = "jobSubmitIp";
    public static final String JOB_START_TIME = "JobSubmitTime";
    public static final String APPLICATIONID = "applicationId";
    public static final String YARN_RM_ADDRESS = "YARN_RM_ADDRESS";
    public static final String TASK_ID = "taskId";
    public static final String TASK_IP = "taskIp";
    public static final String ATTEMPTID = "attemptId";
    public static final String TASK_START_TIME = "taskStartTime";
    public static final String TASK_TYPE = "taskType";
    public static final String MAP  = "map";
    public static final String REDUCE = "reduce";
    public static final String TASK_SPLIT_START = "TASK_SPLIT_START";
    public static final String TASK_SPLIT_LEN = "TASK_SPLIT_LEN";

    public static final  String JOB_TYPE = "JOB_TYPE";
    public static final  String JOB_TYPE_STR = "JOB_TYPE_STR";
    public static final String JOB_NAME = "JOB_NAME";
    public static final String DATAMAP_SEND_RST_DEFAULT = "metric_dmq_send_result";
    public static final String DATAMAP_SEND_RST = "monitor_name_datamap_send_reuslt";
    public static final String DATAMAP_REJECT_RST_KEY = "monitor_datamap_reject_rst_key";
    public static Integer JOB_TYPE_MR = 1;
    public static Integer JOB_TYPE_SPARK = 2;



    public static final String CLOUDHDFS_CLIENT_APP_ID="cloudhdfs.client.app.id";
    public static final String CLOUDHDFS_CLIENT_CURRENT_PATH="cloudhdfs.client.current.path";
    public static final String CLOUDHDFS_CLIENT_CURRENT_WRITING_PATH="cloudhdfs.client.current.writing.path";

    public static final String DATA_REFRESH_KEY="vnn_data_refresh_key";

    public static final String DATA_REFRESH_DEFAULT="metric_vnn_data_refresh";

    public static final String INVALID_DELETE_REPORT_KEY="invalid_delete_report_key";
    public static final String INVALID_DELETE_REPORT_DEFAULT="vnn_metric_invalid_delete";

    public static final String PCG_OLD_VERSION = "pcg_old_version";

}
