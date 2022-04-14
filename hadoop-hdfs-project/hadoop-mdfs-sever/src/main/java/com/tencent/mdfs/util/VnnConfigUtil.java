package com.tencent.mdfs.util;


import static com.tencent.mdfs.config.Config.CONFIG_KEY_DB_REFRESH_PERIOD;
import static com.tencent.mdfs.config.Config.CONFIG_KEY_DB_REFRESH_PERIOD_DEFAULT;
import static com.tencent.mdfs.config.Config.CONFIG_METRIC_VNN_SERVER_MONITOR;
import static com.tencent.mdfs.config.Config.CONFIG_METRIC_VNN_SERVER_MONITOR_DEFAULT;
import static com.tencent.mdfs.config.Config.CONFIG_METRIC_VNN_VNN2NN_MONITOR;
import static com.tencent.mdfs.config.Config.CONFIG_METRIC_VNN_VNN2NN_MONITOR_DEFAULT;
import static com.tencent.mdfs.config.Config.CONFIG_METRIC_VNN_CONNPOOL_MONITOR;
import static com.tencent.mdfs.config.Config.CONFIG_METRIC_VNN_CONNPOOL_MONITOR_DEFAULT;


import com.qq.cloud.taf.server.config.ConfigurationManager;

import com.tencent.mdfs.config.Config;
import com.tencent.mdfs.storage.VnnConfigServiceManager;
import com.tencent.mdfs.config.Constants;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * create:lichunxiao
 * Date:2019-03-14
 */
public class VnnConfigUtil {


    private static final Logger logger = LogManager.getLogger("VnnConfigUtil");

    private static Configuration conf = new Configuration();

    private static Properties properties = new Properties();

    private static Properties metrics = new Properties();

    private static boolean isAuditLogOn = true;

    private static final String VNN_CONFIG_FILE = "vnn_server_config.properties";
    private static final String PROTECT_DIR_CONFIG_FILE = "protect_dir_config_file.properties";
    private static final String VNN_METRIC_CONFIG_FILE = "vnn_server_metric_config.properties";



    private static Set<String> protectDirs = Collections.synchronizedSet(new HashSet<String>());


    static {
        init();
    }


    private static void init() {
        conf = new Configuration();

        InputStream core = loadInputStreamFromRemoteOrLocal("core-site.xml");

        if (core != null) {
            conf.addResource(core);
        }

        InputStream hdfs = loadInputStreamFromRemoteOrLocal("hdfs-site.xml");

        if (hdfs != null) {
            conf.addResource(hdfs);
        }

        if (core != null && hdfs != null) {
            TafLogUtil.log("init hadoop conf done " + core + " " + hdfs);
        } else {
            TafLogUtil.err("hdfs-site.xml or core-site.xml is null");
        }

        loadMainPropertiesConfig();

        loadMetricPropertiesFromServer();

        TafLogUtil.log("VnnConfigUtil init ok");

        loadProtectDirs();


    }

    private static void loadProtectDirs() {


        logger.info("loadProtectDirConfig  start "+ PROTECT_DIR_CONFIG_FILE);


        InputStream in = loadInputStreamFromRemoteOrLocal(PROTECT_DIR_CONFIG_FILE);


        try {

            String s = IOUtils.toString(in);
            logger.info("loadProtectDirConfig str "+s);
            List<String> dirs = IOUtils.readLines(in);

            logger.info("loadProtectDirConfig  size "+ dirs.size());

            for (String d : dirs) {
                protectDirs.add(d);
                protectDirs.add(d + "/");
            }

        }catch (Exception e){
            e.printStackTrace();
        }



        Set<String> appCodes = VnnConfigServiceManager.getInstance().getAppNames();

        if (appCodes != null) {
            logger.info("appCodes size "+appCodes.size());
            for (String d : appCodes) {
                if(!StringUtils.isEmpty(d)){
                    protectDirs.add("/user/"+d);
                    protectDirs.add("/user/"+d + "/");

                    protectDirs.add("/user/"+d+"/hive");
                    protectDirs.add("/user/"+d + "/hive/");

                    protectDirs.add("/user/"+d+"/data");
                    protectDirs.add("/user/"+d + "/data/");
                }
            }
        }

        for(String item:protectDirs){
            logger.info("protect dir: "+item);
        }

        logger.info("protectDirLevelOn  : {} ",protectDirLevelOn());
        logger.info("loadProtectDirs size : {} ", protectDirs.size());
    }

    public static boolean isAuditLogOn() {
        return getBoolValue(Config.CONFIG_AUDIT_LOG_ON, true);
    }

    public static boolean isAuditLogLocalOn() {
        return getBoolValue(Config.CONFIG_AUDIT_LOG_LOCAL_ON, true);
    }


    public static String getLogLevel() {
        return getStringValue(Config.CONFIG_LOG_LEVEL, "INFO");
    }

    public static String get(String key) {
        return getStringValue(properties, key, null);
    }

    public static String get(String key, String defaultValue) {
        return getStringValue(properties, key, defaultValue);
    }

    public static String getStringValue(String key, String defaultValue) {
        return getStringValue(properties, key, defaultValue);

    }

    public static boolean getBoolValue(String key, boolean defaultValue) {
        return getBoolValue(properties, key, defaultValue);

    }

    public static long getLongValue(String key, long defaultValue) {
        return getLongValue(properties, key, defaultValue);

    }

    public static int getIntValue(String key, int defaultValue) {
        return getIntValue(properties, key, defaultValue);

    }

    private static String getStringValue(Properties properties, String key, String defaultValue) {
        try {
            Object v = properties.get(key);
            if (v != null) {
                return (String) v;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return defaultValue;

    }

    private static boolean getBoolValue(Properties properties, String key, boolean defaultValue) {
        try {
            String v = (String) properties.get(key);
            if (v != null) {
                return Boolean.parseBoolean(v);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return defaultValue;

    }

    private static long getLongValue(Properties properties, String key, long defaultValue) {
        try {
            String v = (String) properties.get(key);
            if (v != null) {
                return Long.parseLong(v);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return defaultValue;

    }

    private static int getIntValue(Properties properties, String key, int defaultValue) {
        try {
            String v = (String) properties.get(key);
            if (v != null) {
                return Integer.parseInt(v);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return defaultValue;

    }

    public static Class getClassByName(String key) {
        if( properties == null ){
            return  null;
        }
        if( properties.getProperty( key ) == null ){
            return null;
        }
        try {
            return Class.forName( properties.getProperty( key ) );
        } catch (ClassNotFoundException e) {
            logger.error( "Class not found:{}",properties.get(key) );
        }
        return null;
    }

    public static void loadMetricPropertiesFromServer() {

        logger.info("loadMetricPropertiesFromServer");

        InputStream in = loadInputStreamFromRemoteOrLocal(VNN_METRIC_CONFIG_FILE);

        if (in != null) {
            boolean ret = false;
            try {
                metrics.load(in);
                ret = true;
            } catch (Exception e) {
                e.printStackTrace();
                logger.error(" loadMetricPropertiesFromServer error {} ", e);
            }
            if (ret) {

                VnnLogUtil.log("loadMetricPropertiesFromServer----ok..." + isAuditLogOn());
            }
        } else {
            logger.error(" loadMetricPropertiesFromServer error ,input stream is null ");
        }
    }


    public static void loadMainPropertiesConfig() {

        logger.info("loadMainPropertiesConfig {} start ", VNN_CONFIG_FILE);


        InputStream in = loadInputStreamFromRemoteOrLocal(VNN_CONFIG_FILE);

        if (in != null) {
            Properties p = new Properties();
            boolean ret = false;
            try {
                p.load(in);
                ret = true;
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("loadMainPropertiesConfig error ", e);
            }
            if (ret) {

                properties = p;

                TafLogUtil.log("loadMainPropertiesConfig----ok..." + isAuditLogOn());
            } else {
                throw new RuntimeException("loadMainPropertiesConfig error ");
            }
        } else {
            TafLogUtil.err(" loadMainPropertiesConfig error ,{} input stream is null " + VNN_CONFIG_FILE);
            throw new RuntimeException("loadMainPropertiesConfig error ");
        }
    }



    public static boolean isDebugOn() {
        return getBoolValue(Config.CONFIG_DEBUG_LOG_ON, false);
    }

    public static Configuration loadHadoopConfig() {
        return conf;
    }

    public static Configuration getHadoopConfig() {
        return conf;
    }

    public static boolean isTdcShareMode(final Configuration conf) {
        return getBoolValue(Config.CONFIG_TDC_SHARE_MODE, false);
    }

    public static int getMaxDirLevel() {
        return getIntValue(Config.CONFIG_MAX_LEVEL, 20);
    }

    public static int getMaxDirCheckLevel() {
        return getIntValue(Config.CONFIG_MAX_LEVEL, 20);
    }


    public static String getVNNServerMonitorName() {
        return getStringValue(metrics, CONFIG_METRIC_VNN_SERVER_MONITOR,
                CONFIG_METRIC_VNN_SERVER_MONITOR_DEFAULT);
    }

    public static String getConnPoolMonitorName() {
        return getStringValue(metrics, CONFIG_METRIC_VNN_CONNPOOL_MONITOR,
                CONFIG_METRIC_VNN_CONNPOOL_MONITOR_DEFAULT);
    }

    public static String getVnn2NNMonitorName() {
        return getStringValue(metrics, CONFIG_METRIC_VNN_VNN2NN_MONITOR,
                CONFIG_METRIC_VNN_VNN2NN_MONITOR_DEFAULT);
    }

    public static String getVnnMonitorName() {
        return getStringValue(metrics,Constants.VNN_MONITOR_NAME, Constants.VNN_MONITOR_NAME_DEFAULT);
    }

    public static int getDBRefreshPeriod() {
        return getIntValue(properties, CONFIG_KEY_DB_REFRESH_PERIOD,
                CONFIG_KEY_DB_REFRESH_PERIOD_DEFAULT);
    }
    public static InputStream loadInputStreamFromRemoteOrLocal(String file) {
        InputStream in = null;
        in = loadLocalInputStream(file);
        return in;
    }


    public static InputStream loadLocalInputStream(String name) {
        InputStream inputStream = VnnConfigUtil.class.getClassLoader().
                getResourceAsStream(name);
        return inputStream;
    }

    public static String getVnnRefreshMonitorName() {
        return conf.get(Constants.DATA_REFRESH_KEY, Constants.DATA_REFRESH_DEFAULT);
    }


    public static String getDatamapSendRst() {
        return conf.get(Constants.DATAMAP_SEND_RST, Constants.DATAMAP_SEND_RST_DEFAULT);
    }

    public static String getDatamapRejected() {
        return conf.get(Constants.DATAMAP_REJECT_RST_KEY, "metric_rejected_msg");
    }

    public static String getDMQApp() {
        return conf.get(Constants.DATA_MAP_DMQ_APP_KEY, Constants.DATA_MAP_DMQ_APP_DEFAULT);
    }

    public static String getDMQServer() {
        return conf.get(Constants.DATA_MAP_DMQ_SERVER_KEY, Constants.DATA_MAP_DMQ_SERVER_DEFAULT);
    }

    public static String getDMQFile() {
        return conf.get(Constants.DATA_MAP_DMQ_FILE_KEY, Constants.DATA_MAP_DMQ_FILE_DEFAULT);
    }

    public static String getAuditDMQApp() {
        return getStringValue(metrics, Constants.AUDIT_DMQ_APP_KEY, Constants.AUDIT_DMQ_APP_DEFAULT);
    }

    public static String getAuditDMQServer() {
        return getStringValue(metrics, Constants.AUDIT_DMQ_SERVER_KEY, Constants.AUDIT_DMQ_SERVER_DEFAULT);
    }

    public static String getAuditDMQFile() {
        return getStringValue(metrics, Constants.AUDIT_DMQ_FILE_KEY, Constants.AUDIT_DMQ_FILE_DEFAULT);
    }

    public static String getDMQLocator() {
        return SeerUtil.getLocator();
    }

    public static String getDMQObjName() {
        return conf.get(Constants.DATA_MAP_DMQ_OBJ_NAME_KEY, Constants.DATA_MAP_DMQ_OBJ_NAME_DEFAULT);
    }

    public static List<String> getConfigList() {

        InputStream in = loadLocalInputStream("config.files");

        List<String> ret = new ArrayList<>();

        if (in != null) {

            try {
                ret = IOUtils.readLines(in, "utf-8");
            } catch (IOException e) {
                e.printStackTrace();

                TafLogUtil.info("getConfigList error  " + e);

            }


        } else {
            TafLogUtil.err("loadRemoteConfigAsStream config.files is null");
        }

        TafLogUtil.info("loadServerConfigByFileName files size " + ret.size());

        return ret;
    }


    public static String getConfigName(String file) {
        return ConfigurationManager.getInstance().getserverConfig().getBasePath() + "/conf/" + file;
    }


    public static int getDMQBatchSize() {
        return getIntValue("dmq.batch.send.size", 1024);
    }

    public static Set<String> getProtectDirs() {
        return protectDirs;

    }

    public static void refresh() {
        logger.info("refresh config ");
        init();
    }

    public static boolean isProtectDirOn() {
        return getBoolValue("cloudhdfs.protect.dir.on", true);
    }

    public static boolean protectDirLevelOn() {
        return getBoolValue("cloudhdfs.protect.dir.level.on", true);
    }

    public static int getProtectDirLevel() {
        return getIntValue("cloudhdfs.protect.dir.level", 2);
    }

    public static String getNameNodeServerAddr() {
        try {
            return getStringValue("cloudhdfs.namenode.server.address", InetAddress.getLocalHost().getHostAddress());
        } catch (UnknownHostException e) {
            return "127.0.0.1";
        }
    }

    public static int getHandlerNum(){
        return getIntValue("cloudhdfs.namenode.server.handles",1024);
    }

    public static int getNameNodeServerPort() {
        return getIntValue("cloudhdfs.namenode.server.port",9002);
    }
}
