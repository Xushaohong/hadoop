package com.tencent.mdfs.provider.util;


import com.tencent.mdfs.provider.dao.AppDirRelationDao;
import com.tencent.mdfs.provider.dao.AppInfoDao;
import com.tencent.mdfs.provider.dao.AppRelationDao;
import com.tencent.mdfs.provider.dao.AppRootDao;
import com.tencent.mdfs.provider.dao.DirInfoDao;
import com.tencent.mdfs.provider.dao.NameNodeIpDao;
import com.tencent.mdfs.provider.pojo.AppDirRelation;
import com.tencent.mdfs.provider.pojo.AppInfo;
import com.tencent.mdfs.provider.pojo.AppRelation;
import com.tencent.mdfs.provider.pojo.AppRoot;
import com.tencent.mdfs.provider.pojo.DirInfo;
import com.tencent.mdfs.provider.pojo.NameNodeIp;
import java.util.List;

/**
 * create:chunxiaoli
 * Date:4/20/18
 */
public class DaoUtil {
    private static AppInfoDao appInfoDao = new AppInfoDao();
    private static DirInfoDao dirInfoDao = new DirInfoDao();
    private static AppDirRelationDao appDirRelationDao = new AppDirRelationDao();
    private static AppRelationDao appRelationDao = new AppRelationDao();
    private static AppRootDao appRootDao = new AppRootDao();
    private static NameNodeIpDao nameNodeIpDao = new NameNodeIpDao();


    public static int add(AppInfo appInfo){
        return appInfoDao.insert(appInfo);
    }

    public static int add(DirInfo dirInfo){
        return dirInfoDao.insert(dirInfo);
    }

    public static int add(AppDirRelation appDirRelation){
        return appDirRelationDao.insert(appDirRelation);
    }

    public static List<AppInfo> queryAllApps(){
        return appInfoDao.selectAll();
    }

    public static List<DirInfo> queryAllDirs(){
        return dirInfoDao.selectAll();
    }

    public static List<AppDirRelation> queryAllAppDirs(){
        return appDirRelationDao.selectAll();
    }

    public static List<AppRelation> queryAllAppRelations() {
        return appRelationDao.selectAll();
    }

    public static List<AppRoot> listAll(){return appRootDao.listAll(); }

    public static boolean updateNNIp( String clusterId,String availableNn ){
        return nameNodeIpDao.update( new NameNodeIp( clusterId,availableNn ) );
    }
}
