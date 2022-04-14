package com.tencent.mdfs.util;

import com.google.common.base.Strings;
import com.qq.cloud.taf.client.Communicator;
import com.qq.cloud.taf.client.CommunicatorConfig;
import com.qq.cloud.taf.client.CommunicatorFactory;
import com.tencent.mdfs.provider.orm.SpecialPathMapper;
import com.tencent.mdfs.provider.pojo.SpecialPath;
import com.tencent.mdfs.provider.util.MyBatisUtil;
import com.tencent.mdfs.servant.NNConfigServant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Resource;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalCache {
    @Resource
    static SpecialPathMapper specialPathMapper =  MyBatisUtil.getMapper(SpecialPathMapper.class);
    volatile public static Map<String,NnInfo> nnChoises = new ConcurrentHashMap<>();
    static NNConfigServant nnConfigServant = null;
    static Logger logger = LoggerFactory.getLogger( LocalCache.class );
    public static Map<String,String> nnAddrCache = new ConcurrentHashMap<>();
    public static Map<String,String> nnAddrClusterCache = new ConcurrentHashMap<>();
    public static Map<String,SpecialPath> specialPathMap = new ConcurrentHashMap<>();
    public static Map<String,String> nnVersion = new ConcurrentHashMap<>();

    public static String getNNAddr( String clusterId ){
        return nnAddrCache.get( clusterId );
    }
    public static String getClusterId(String ipPort){
        return nnAddrClusterCache.get( ipPort );
    }


    static {
        try {
            CommunicatorConfig config = new CommunicatorConfig();
            config.setLocator(SeerUtil.getLocator());
            Communicator communicator = CommunicatorFactory.getInstance().getCommunicator(config);
            String objName = "hadoopconfig.ConfigServer.NNConfigQryObj";
            nnConfigServant = communicator.stringToProxy(NNConfigServant.class, objName);
            refreshLocalCache();
            refreshSpecialPath();
            ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
            executorService.scheduleAtFixedRate(
                new Runnable() {
                    @Override
                    public void run() {
                        try {
                            refreshLocalCache();
                        } catch (Exception e) {
                            logger.warn("refreshLocalCache failed", e);
                        }
                        try {
                            refreshSpecialPath();
                        } catch (Exception e) {
                            logger.warn("refreshSpecialPath failed", e);
                        }
                    }
                }, 600, 600, TimeUnit.SECONDS
            );
        }catch (Throwable e){
            VnnLogUtil.err("init failed"+e);
            e.printStackTrace();
        }
    }

    private static void refreshSpecialPath() {
        try {
            List<SpecialPath> list = specialPathMapper.listAll();
            if (list != null && list.size() > 0) {
                for (SpecialPath specialPath : list) {
                    logger.info("SpecialPath:{}",specialPath);
                    specialPathMap.put(specialPath.getPathPre(), specialPath);
                }
            } else {
                logger.warn("specialPathMapper.listAll is null");
            }
        }catch (Exception e){
            logger.warn("refreshSpecialPath failed",e);
        }
    }

    static void refreshLocalCache(){
        Map<String,List<String>> nnInfos = nnConfigServant.getAllNameNodes();
        Map<String,String> nnAddrClusterCacheTmp = new ConcurrentHashMap<>();
        Map<String,NnInfo> nnChoisesTmp = new ConcurrentHashMap<>();
        for( Map.Entry<String,List<String>> entry : nnInfos.entrySet() ){
            String key = entry.getKey();
            List<String> nnAddrs = convert2IpPort( entry.getValue() );
            NnInfo localNNInfo = nnChoises.get( key );
            if( localNNInfo == null ){
                nnChoisesTmp.put( key,new NnInfo(nnAddrs) );
                //first addr as active
                if( nnAddrCache!= null && nnAddrs.size() > 0) {
                    nnAddrCache.put(key,nnAddrs.get(0) );
                }
            }else{
                nnChoisesTmp.put( key,new NnInfo(nnAddrs,localNNInfo.getCurrentIndex(),localNNInfo.getLastFailTime()) );
            }

            // ipport->clusterId
            for( String nnAddr : nnAddrs ){
                nnAddrClusterCacheTmp.put( nnAddr,key );
            }
        }
        nnChoises = nnChoisesTmp;
        nnAddrClusterCache = nnAddrClusterCacheTmp;
        List<Map<String,String>> nnVersions = nnConfigServant.getAllNameNodeVersion();
        if(logger.isDebugEnabled()){
            logger.debug("getAllNameNodeVersion:{}",nnVersions);
        }
        if(nnVersions == null){
            logger.error("nnVersion is null");
            return;
        }
        for(Map<String,String> oneNN : nnVersions){
            if(isIp(oneNN.get("active_nn"))){
                nnVersion.put(oneNN.get("active_nn").trim(),oneNN.get("version").trim());
            }
            if(isIp(oneNN.get("standby_nn"))){
                nnVersion.put(oneNN.get("standby_nn").trim(),oneNN.get("version").trim());
            }
            if(isIp(oneNN.get("secondary_nn"))){
                nnVersion.put(oneNN.get("secondary_nn").trim(),oneNN.get("version").trim());
            }
        }
    }

    private static boolean isIp(String ip){
        if(Strings.isNullOrEmpty( ip )){
            return false;
        }
        return ip.indexOf(".") > -1;
    }

    private static List<String> convert2IpPort(List<String> value) {
        List<String> ipPorts = new ArrayList<>();
        if( value == null || value.size() == 0){
            return new ArrayList<>();
        }
        for( String ip : value ){
            ipPorts.add( ip + ":9000" );
        }
        return ipPorts;
    }




    public static class NnInfo{
        public NnInfo(List<String> nnAddrs){
            this.nnAddrs = nnAddrs;
        }

        public NnInfo(List<String> nnAddrs,int currentIndex,Map<Integer,Long> lastFailTime){
            this.nnAddrs = nnAddrs;
            this.currentIndex  = currentIndex;
            this.lastFailTime = lastFailTime;
        }

        public NnInfo(){}
        private int currentIndex = 0;
        private List<String> nnAddrs = new ArrayList<>();
        private Map<Integer,Long> lastFailTime = new ConcurrentHashMap<>();

        public int getCurrentIndex() {
            return currentIndex;
        }

        public void setCurrentIndex(int currentIndex) {
            this.currentIndex = currentIndex;
        }

        public List<String> getNnAddrs() {
            return nnAddrs;
        }

        public void setNnAddrs(List<String> nnAddrs) {
            this.nnAddrs = nnAddrs;
        }

        public Map<Integer, Long> getLastFailTime() {
            return lastFailTime;
        }

        public void setLastFailTime(Map<Integer, Long> lastFailTime) {
            this.lastFailTime = lastFailTime;
        }
    }
}
