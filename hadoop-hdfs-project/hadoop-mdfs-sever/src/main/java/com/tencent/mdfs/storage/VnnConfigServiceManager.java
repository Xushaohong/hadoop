package com.tencent.mdfs.storage;

import com.tencent.mdfs.entity.RouteInfo;
import com.tencent.mdfs.permission.PermissionUtil;
import com.tencent.mdfs.provider.pojo.AppDirRelation;
import com.tencent.mdfs.provider.pojo.AppInfo;
import com.tencent.mdfs.provider.pojo.AppRelation;
import com.tencent.mdfs.provider.pojo.AppRoot;
import com.tencent.mdfs.provider.pojo.DirInfo;
import com.tencent.mdfs.provider.util.DaoUtil;
import com.tencent.mdfs.provider.util.MyBatisUtil;
import com.tencent.mdfs.router.RouteUtil;
import com.tencent.mdfs.util.LocalCache;
import com.tencent.mdfs.util.MMUtil;
import com.tencent.mdfs.util.VnnConfigUtil;
import com.tencent.mdfs.util.VnnLogUtil;
import com.tencent.mdfs.config.Constants;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.hadoop.conf.Configuration;

/**
 * create:chunxiaoli
 * Date:4/8/18
 */
public class VnnConfigServiceManager implements VnnConfigService {

    public static ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    public static ReentrantReadWriteLock appDirReadWriteLock = new ReentrantReadWriteLock();

    static {
        VnnConfigServiceManager.getInstance();
    }

    private static VnnConfigServiceManager instance;

    private Configuration conf;

    private Map<String, RouteInfo> dirPath2RouteMap = new ConcurrentHashMap<>();

    private Map<Long, DirInfo> dirInfoMap = new ConcurrentHashMap<>();

    private Map<String,AppDirRelation> appDirRelationMap = new ConcurrentHashMap<>();

    private Map<Long,List<AppDirRelation>> appDirMap = new ConcurrentHashMap<>();

    private Map<Long,List<Long>> appRelationMap = new ConcurrentHashMap<>();

    private List<AppInfo> apps;

    private List<AppDirRelation> appDirs = new ArrayList<>();

    private List<DirInfo> dirsList = new ArrayList<>();


    private int maxDirLevel;

    private int maxDirCheckLevel;


    private  List<String> whiteList=new CopyOnWriteArrayList<>();
    private  List<String> blackList=new CopyOnWriteArrayList<>();


    private final Set<String> dirWhiteList = Collections.synchronizedSet(new HashSet<String>()) ;
    private final Set<String> dirBlackList = Collections.synchronizedSet(new HashSet<String>());

    public Map<Long,AppRoot> appRootMap = new HashMap<>();
    private final Set<String> appCodes = Collections.synchronizedSet(new HashSet<String>());

    //private final StampedLock

    public List<AppDirRelation> getAppDirRelationByDirId(Long  dirId ){
        if( dirId == null ){
            return null;
        }
        try {
            appDirReadWriteLock.readLock().lock();
            return appDirMap.get( dirId );
        }finally {
            appDirReadWriteLock.readLock().unlock();
        }
    }


    public AppDirRelation getAppDirRelation(String  dirIdAndAppId ){
        try {
            appDirReadWriteLock.readLock().lock();
            return appDirRelationMap.get(dirIdAndAppId);
        }finally {
            appDirReadWriteLock.readLock().unlock();
        }
    }

    public List<Long> getAppParents(long appId) {
        try {
            appDirReadWriteLock.readLock().lock();
            return appRelationMap.get(appId);
        }finally {
            appDirReadWriteLock.readLock().unlock();
        }
    }
    //private final Set<String> dirWhiteList=new HashSet<>();

    private VnnConfigServiceManager() {

        VnnLogUtil.log("VnnConfigServiceManager init ");

        this.maxDirLevel= VnnConfigUtil.getMaxDirLevel();
        this.maxDirCheckLevel=VnnConfigUtil.getMaxDirCheckLevel();

        loadData();

        int dbRefresh= VnnConfigUtil.getDBRefreshPeriod();

        VnnLogUtil.log("VnnConfigServiceManager init dbRefresh "+dbRefresh);


        Executors.newScheduledThreadPool(1, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t =  new Thread(r);
                t.setName("VnnConfigServiceManager-refresh");
                return t;
            }
        }).scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {

                long start = System.currentTimeMillis();

                try{
                    VnnLogUtil.log("reloadDataOld start");


                    reload();

                    long end  = System.currentTimeMillis();

                    MMUtil.reportVNNDataRefresh(dirPath2RouteMap.size(),(end-start),0);

                    VnnLogUtil.log("reloadDataOld end");

                    VnnLogUtil.log("dirsList size:" + dirsList.size());

                    VnnLogUtil.log("apps size:" + apps.size());

                    VnnLogUtil.log("appDirs size:" + appDirs.size());

                    VnnLogUtil.log("dirPath2RouteMap size:" + dirPath2RouteMap.size());

                    VnnLogUtil.log("appDirRelations size:" + appDirRelationMap.size());

                    VnnLogUtil.log("appDirMap size:" + appDirMap.size());

                    VnnLogUtil.log("appRoot size:" + appRootMap.size());

                    VnnLogUtil.log("appRelations size:" + appRelationMap.size());

                }catch (Throwable t){
                    t.printStackTrace();
                    VnnLogUtil.err("reload vnn data error "+t);

                    MMUtil.reportVNNDataRefresh(dirPath2RouteMap.size(),(System.currentTimeMillis()-start),1);

                }



            }
        }, 0,dbRefresh, TimeUnit.SECONDS);

        if(VnnConfigUtil.isDebugOn()){
            debug();
        }

        VnnLogUtil.log("VnnConfigServiceManager init ok");
    }

    public synchronized static VnnConfigServiceManager getInstance() {
        if (instance == null) {
            instance = new VnnConfigServiceManager();
        }
        return instance;
    }


    private void reload() {

        long start = System.currentTimeMillis();

        MyBatisUtil.clearCache();



        List<String> whiteListNew = StorageUtil.readRemoteLines(Constants.WHITE_LIST_DATA_NAME);
        List<String> blackListNew = StorageUtil.readRemoteLines(Constants.BLACK_LIST_DATA_NAME);



        List<DirInfo> dirsListNew = StorageUtil.loadDirInfos();

        if(dirsListNew==null||dirsListNew.isEmpty()){
            VnnLogUtil.err("StorageUtil.loadDirInfos is null or empty ");
            return;
        }

        Map<String, RouteInfo> dirPath2RouteMapNew = new ConcurrentHashMap<>();

        Map<Long, DirInfo> dirInfoMapNew = new ConcurrentHashMap<>();

        PermissionUtil.createDirMap(false,dirsListNew,dirPath2RouteMapNew,dirInfoMapNew);



        List<AppInfo> appsNew = StorageUtil.loadAppInfos();

        if(appsNew==null||appsNew.isEmpty()){
            VnnLogUtil.err("StorageUtil.loadAppInfos is null or empty ");
            return;
        }


        List<AppDirRelation> appDirsNew = StorageUtil.loadAppDirRelations();


        if(appDirsNew==null||appDirsNew.isEmpty()){
            VnnLogUtil.err("StorageUtil.loadAppDirRelations is null or empty ");
        }

        Map<String,AppDirRelation> appDirRelationMapNew = new HashMap<>();
        Map<Long,List<AppDirRelation>> appDirMapNew = new HashMap<>();
        Map<Long,List<Long>> appRelationMapNew = new HashMap<>();
        List<AppRelation> appRelations = StorageUtil.loadAppRelations();
        List<AppDirRelation> appDirRelations = StorageUtil.loadAppDirRelations();
        PermissionUtil.createPermissionMap( appDirRelations,appRelations ,appDirRelationMapNew,appRelationMapNew, appDirMapNew);

        List<AppRoot> appRoots = DaoUtil.listAll();
        Map<Long,AppRoot> appRootMapNew = new HashMap<>();
        PermissionUtil.createRootMap( appRoots,appRootMapNew );

        doRefreshData(appsNew,dirsListNew,appDirsNew,whiteListNew,blackListNew,
                dirPath2RouteMapNew,dirInfoMapNew,appDirRelationMapNew,appRelationMapNew,appRootMapNew,appDirMapNew);




        long end = System.currentTimeMillis();
        VnnLogUtil.log("reload use time " + (end - start));


    }

    private void reloadDataOld() {
        long start = System.currentTimeMillis();
        this.whiteList = StorageUtil.readRemoteLines("white_list_data.txt");
        if (this.whiteList != null) {
            this.dirWhiteList.addAll(this.whiteList);
        }
        this.dirsList = StorageUtil.loadDirInfos();
        this.dirPath2RouteMap = new ConcurrentHashMap<>();
        this.dirInfoMap = new ConcurrentHashMap<>();

        VnnLogUtil.log("before create map "+dirPath2RouteMap.size());
        PermissionUtil.createDirMap(true,this.dirsList, this.dirPath2RouteMap, this.dirInfoMap);
        this.apps = StorageUtil.loadAppInfos();
        this.appDirs = StorageUtil.loadAppDirRelations();
        long end = System.currentTimeMillis();
        VnnLogUtil.log("reloadDataOld use time " + (end - start));
    }

    //fixme not safe for list
    private void doRefreshData(
            List<AppInfo> appsNew,
            List<DirInfo> dirsListNew,
            List<AppDirRelation> appDirsNew,
            List<String> whiteListNew,
            List<String> blackListNew,
            Map<String, RouteInfo> dirPath2RouteMapNew,
            Map<Long, DirInfo> dirInfoMapNew,
            Map<String,AppDirRelation> appDirRelationMapNew,
            Map<Long,List<Long>> appRelationMapNew,
            Map<Long,AppRoot> appRootMapNew,Map<Long,List<AppDirRelation>> appDirMapNew){


        long start = System.currentTimeMillis();



        apps.clear();
        apps.addAll(appsNew);

        initAppCodes();

        dirsList.clear();
        dirsList.addAll(dirsListNew);

        appDirs.clear();
        appDirs.addAll(appDirsNew);

        whiteList.clear();
        if(whiteListNew != null) {
            whiteList.addAll(whiteListNew);
        }

        blackList.clear();
        if(blackListNew != null ) {
            blackList.addAll(blackListNew);
        }

        dirWhiteList.clear();
        dirWhiteList.addAll(whiteList);

        dirBlackList.clear();
        dirBlackList.addAll(blackList);

        try {
            readWriteLock.writeLock().lock();
            dirPath2RouteMap = dirPath2RouteMapNew;
        }finally {
            readWriteLock.writeLock().unlock();
        }
        dirInfoMap = dirInfoMapNew;

        try{
            appDirReadWriteLock.writeLock().lock();
            appDirRelationMap = appDirRelationMapNew;
            appRelationMap = appRelationMapNew;
            appRootMap = appRootMapNew;
            appDirMap = appDirMapNew;
        }finally {
            appDirReadWriteLock.writeLock().unlock();
        }
        long end = System.currentTimeMillis();
        VnnLogUtil.log("doRefreshData use time " + (end - start));


    }

    private void loadData() {

        long start = System.currentTimeMillis();

        whiteList = StorageUtil.readServerOrAppConfig(Constants.WHITE_LIST_DATA_NAME);
        blackList = StorageUtil.readServerOrAppConfig(Constants.BLACK_LIST_DATA_NAME);

        if(whiteList!=null){
            dirWhiteList.addAll(whiteList);
        }

        if(whiteList!=null){
            dirBlackList.addAll(blackList);
        }

        dirsList = StorageUtil.loadDirInfos();

        dirPath2RouteMap = new ConcurrentHashMap<>();
        dirInfoMap = new ConcurrentHashMap<>();

        PermissionUtil.createDirMap(false,dirsList,dirPath2RouteMap,dirInfoMap);

        apps = StorageUtil.loadAppInfos();

        List<AppRelation> appRelations = StorageUtil.loadAppRelations();
        initAppCodes();


        List<AppDirRelation> appDirRelations = StorageUtil.loadAppDirRelations();

        PermissionUtil.createPermissionMap( appDirRelations,appRelations ,appDirRelationMap,appRelationMap,appDirMap );

        List<AppRoot> appRoots = DaoUtil.listAll();

        PermissionUtil.createRootMap( appRoots,appRootMap );
        long end = System.currentTimeMillis();

        VnnLogUtil.log("loadDBData use time " + (end - start));


    }

    private void debug() {
        VnnLogUtil.log("dirsList size:" + dirsList.size());

        VnnLogUtil.log("apps size:" + apps.size());

        VnnLogUtil.log("appDirs size:" + appDirs.size());

        VnnLogUtil.log("dirPath2RouteMap size:" + dirPath2RouteMap.size());

       /* for (DirInfo k : dirsList) {
            VnnLogUtil.log("dirsList dir => :"+k );
        }*/

        for (AppDirRelation k : appDirs) {
            VnnLogUtil.log("appDirs  => :"+k );
        }

        for (AppInfo k : apps) {
            VnnLogUtil.log("apps => :"+k + ": " + k.getAppName());
        }

        VnnLogUtil.log("white list size:" + dirWhiteList.size());
        for (String k : dirWhiteList) {
            VnnLogUtil.log( "dirWhiteList dir:" + k);
        }

        VnnLogUtil.log("dirBlackList list size:" + dirBlackList.size());
        for (String k : dirBlackList) {
            VnnLogUtil.log( "dirBlackList dir:" + k);
        }


        VnnLogUtil.log("maxDirLevel :"+maxDirLevel);
        VnnLogUtil.log("maxDirCheckLevel :"+maxDirCheckLevel);
    }

    @Override
    public RouteInfo getRouteInfo(String path) {

        RouteInfo ret;

        //first find with whole path
        try {
            readWriteLock.readLock().lock();
            ret = dirPath2RouteMap.get(path);
        }finally {
            readWriteLock.readLock().unlock();
        }


        if (ret == null) {
            //iterator parent dir
            ret = RouteUtil.getMatchRouteInfo(dirPath2RouteMap, maxDirLevel, path);

        }else {
            ret.setMatchType(RouteInfo.MATCH_TYPE_COMPLETE);
        }

        if( ret != null && ret.getClusterId() != null ){
            if( LocalCache.getNNAddr(ret.getClusterId()) != null ){
                ret.setAddr( LocalCache.getNNAddr(ret.getClusterId()) );
            }else{
                VnnLogUtil.warn("LocalCache.getNNAddr(ret.getClusterId()) is null "+ret.getClusterId());
            }
        }else{
            VnnLogUtil.warn("ret is null || ret.getClusterId is null ret:" + ret);
        }

        if(VnnConfigUtil.isDebugOn()){
            VnnLogUtil.log("getRouteInfo "+path+"==> ret: "+ret+" "+dirPath2RouteMap.size());
        }
        return ret;

    }

    public void getWriteableMap() {

    }

    public int getMaxDirCheckLevel(){
        return maxDirCheckLevel;
    }


    public Configuration getHadoopConf() {
        return VnnConfigUtil.getHadoopConfig();
    }

    public Map<String, RouteInfo> getDirPath2RouteMap() {
        return dirPath2RouteMap;
    }

    public Map<Long, DirInfo> getDirInfoMap() {
        return dirInfoMap;
    }

    public List<AppInfo> getApps() {
        return apps;
    }

    public List<AppDirRelation> getAppDirs() {
        return appDirs;
    }

    public List<DirInfo> getDirsList() {
        return dirsList;
    }

    public int getMaxDirLevel() {
        return maxDirLevel;
    }


    public  Set<String> getDirWhiteList() {
        return dirWhiteList;
    }

    public Set<String> getDirBlackList() {
        return dirBlackList;
    }


    public Set<String> getAppNames(){
        return appCodes;
    }

    public void  initAppCodes(){
        for(AppInfo info:apps){
            appCodes.add(info.getAppCode());
        }
    }
}
