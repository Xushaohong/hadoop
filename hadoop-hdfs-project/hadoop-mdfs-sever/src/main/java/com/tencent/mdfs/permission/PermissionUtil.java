package com.tencent.mdfs.permission;

import com.tencent.mdfs.config.AppIdType;
import com.tencent.mdfs.config.SubmiterType;
import com.tencent.mdfs.entity.DirInfo;
import com.tencent.mdfs.entity.FTItem;
import com.tencent.mdfs.entity.Group;
import com.tencent.mdfs.entity.OpAction;
import com.tencent.mdfs.entity.OpType;
import com.tencent.mdfs.entity.RouteInfo;
import com.tencent.mdfs.entity.UserFtInfo;
import com.tencent.mdfs.provider.pojo.AppDirRelation;
import com.tencent.mdfs.provider.pojo.AppRelation;
import com.tencent.mdfs.provider.pojo.AppRoot;
import com.tencent.mdfs.router.RouteUtil;
import com.tencent.mdfs.storage.VnnConfigServiceManager;
import com.tencent.mdfs.util.VnnConfigUtil;
import com.tencent.mdfs.util.VnnLogUtil;
import com.tencent.mdfs.util.VnnPathUtil;
import com.tencent.mdfs.exception.MigPermissionDenyException;
import com.tencent.mdfs.util.PathUtil;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * create:chunxiaoli
 * Date:2/6/18
 */
public class PermissionUtil {

    public static Set<DirInfo> getDirs(FTItem ftItem) {

        if (ftItem == null) {
            VnnLogUtil.log("ftItem is null ");
            return null;
        }
        List<Group> list = ftItem.getGroups();
        if (list == null) {
            return new HashSet<>();
        }
        Set<DirInfo> ret = new HashSet<>();

        for (Group group : list) {
            Set<DirInfo> dirs = group.getDirs();
            if (dirs != null) {
                ret.addAll(dirs);
            } else {
                VnnLogUtil.err(ftItem.getName() + " dirs is null");
            }
        }
        return ret;
    }

    public static boolean containsDir(String rootDir, OpAction op, Set<DirInfo> dirInfoSet) {

        VnnLogUtil.log("containsDir " + op + " " + dirInfoSet);

        if (dirInfoSet == null || dirInfoSet.isEmpty()) {
            VnnLogUtil.log("dirInfoSet  is null or empty");
            return false;
        }

        for (DirInfo d : dirInfoSet) {
            //fixme
            if (isRootDir(d)) {
                VnnLogUtil.log("get super root  dir ");
                return true;
            }
            if (d == null) {
                VnnLogUtil.warn("invalid dir " + d + " " + op);
                continue;
            }
            if (rootDir.equals(d.getName()) && d.validOp(op.opType)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isRootDir(DirInfo d) {
        return d.getName().equals("/");
    }


    public static boolean hasPermission(OpAction op, final UserFtInfo userFtInfo) {

        VnnLogUtil.debug("hasPermission " + op + " " + userFtInfo);

        if (op == null) {
            VnnLogUtil.debug("opAction is null");
            return true;
        }

        if (userFtInfo == null) {
            VnnLogUtil.log("userFtInfo is null");
            return false;
        }

        if (userFtInfo.getFtInfoSet() == null || userFtInfo.getFtInfoSet().isEmpty()) {
            VnnLogUtil.err("userFtInfo has no ft set ");
            return false;
        }


        if (op.dir == null) {
            VnnLogUtil.log("op dir is null");
            return false;
        }


        final String rootDir = PathUtil.getRootDir(op.dir);

        VnnLogUtil.debug("rootDir of " + op.dir + " is " + rootDir);


        if (rootDir == null) {
            return false;
        }

        for (FTItem ftItem : userFtInfo.getFtInfoSet()) {

            Set<DirInfo> dirs = getDirs(ftItem);

            if (dirs == null || dirs.isEmpty()) {
                continue;
            }

            VnnLogUtil.debug(" get dirs of " + ftItem.getName() + " size " + dirs.size());

            boolean match = containsDir(rootDir, op, dirs);

            if (match) {
                VnnLogUtil.debug(op.dir + "  in  " + ftItem.getName() + " @@@!");
                return true;
            } else {
                VnnLogUtil.log(op.dir + " not in  " + ftItem.getName());
            }
        }
        return false;
    }

    public static boolean inBlackList(OpAction opAction) {
        if (opAction == null) {
            return true;
        }

        final  String dir = VnnPathUtil.resolveLogicPath(opAction.dir);



        Set<String> list = VnnConfigServiceManager.getInstance().
                getDirBlackList();



        if (list == null) {
            VnnLogUtil.log("VnnConfigServiceManager.getInstance().\n" +
                    "getDirBlackList() is null");

            return false;
        }

       /* VnnLogUtil.log("black list size: "+list.size());

        for (String d : list) {
            VnnLogUtil.log("black list dir:"+d);
        }*/

        /*// white list:/a/b/c        dir /a/b/c/de
        for (String d : list) {

             VnnLogUtil.log("black list:"+d+" src: "+dir);

            if (d.equals(dir) || dir.startsWith(d)) {

                VnnLogUtil.log("start with d: "+dir+" d:"+d);

                return true;
            }

        }

        String root = PathUtil.getRootDir(dir);


        boolean ret= list.contains(root);

        VnnLogUtil.debug("root: "+root+" in white list "+ret);*/

        boolean ret = isSubDirMatch(list,dir);

        VnnLogUtil.debug("dir: "+dir+" in black list "+ret);



        return ret;
    }

    private static boolean isSubDirMatch(final Set<String> dirs, final String dir){
        for ( String whiteDir: dirs ){
            if( dir .startsWith( whiteDir ) ){
                return true;
            }
        }
        return false;
        /*if(dirs.contains(dir)){
            return true;
        }

        String[] parts = dir.split(File.separator);



        List<String> list = Arrays.asList(parts);


        int lev = parts.length;

        if(VnnConfigUtil.isDebugOn()){
            VnnLogUtil.debug("parts size "+parts.length+" "+list.size()+" level "+lev);
        }


        do {
            List<String> sub = list.subList(0, lev);

            if(sub.size()<=1){
                break;
            }

            String d = PathUtil.join(sub);


            if (dirs.contains(d)) {


                if(VnnConfigUtil.isDebugOn()){
                    VnnLogUtil.log(" isSubDirMatch d:"+d+" "+sub.size()+" "+lev+" match true");
                }

                return true;

            }else {
                if(VnnConfigUtil.isDebugOn()){
                    VnnLogUtil.log(" isSubDirMatch d:"+d+" "+sub.size()+" "+lev+" match false");
                }
            }

        } while (lev-- > 0);

        return false;*/
    }

    public static boolean inWhiteList(OpAction opAction) {
        if (opAction == null) {
            return true;
        }

        final  String dir = VnnPathUtil.resolveLogicPath(opAction.dir);



        Set<String> list = VnnConfigServiceManager.getInstance().
                getDirWhiteList();



        if (list == null||list.isEmpty()) {
            VnnLogUtil.warn("white list is null or empty not check ");
            return true;
        }



        boolean ret = isSubDirMatch(list,dir);

        VnnLogUtil.debug("dir: "+dir+" in white list "+ret);



        return ret;
    }

    public static boolean isReadable(Byte type) {
        return type == 1;
    }

    public static boolean isReadOp(OpAction opAction) {
        return opAction.opType == OpType.READ;
    }

    public static boolean isWriteOp(OpAction opAction) {
        return opAction.opType == OpType.WRITE;
    }

    public static boolean isWriteable(Byte type) {
        return type == 2;
    }

    public static String getPermissionDenyMsg(Long appId, OpAction op){
        return  "permission deny for :"+appId+" "+op.opType+" "+op.dir+" !";
    }

    public static String getPermissionDenyMsg(String appName, OpAction op){
        return  "permission deny for :"+appName+" "+op.opType+" "+op.dir+" !";
    }


    public static void createDirMap(boolean sleep,List<com.tencent.mdfs.provider.pojo.DirInfo> dirsList,
                                    Map<String, RouteInfo> dirPath2RouteMap,
       Map<Long, com.tencent.mdfs.provider.pojo.DirInfo> dirInfoMap){




        for (com.tencent.mdfs.provider.pojo.DirInfo dir : dirsList) {

            RouteInfo item = RouteUtil.convert(dir);

            if(item!=null){

                dirPath2RouteMap.put(dir.getLogicPath(),item);

                dirInfoMap.put(dir.getDirId(), dir);
            }




            //VnnLogUtil.log(" dirPath2RouteMap.size: "+dirPath2RouteMap.size());
        }
    }



    public static boolean isWriteList(String src) throws MigPermissionDenyException {
        return false;
    }

    public static boolean isPlatform( int type ){
        return type == SubmiterType.VENUS || type == SubmiterType.DSHARE;
    }

    /**
     * could operator all path ,but dfsadmin operate
     * @param appId
     * @return
     */
    public static boolean isRoot(long appId) {
        AppRoot appRoot = VnnConfigServiceManager.getInstance().appRootMap.get( appId );
        if( appRoot != null && appRoot.getType() == AppIdType.DIRROOT.value() ) {
            return true;
        }
        return false;
    }

    public static boolean isAdminOperator() {
        return false;
    }

    /**
     * could do everything
     * @param appId
     * @return
     */
    public static boolean isAdmin(long appId) {
        AppRoot appRoot = VnnConfigServiceManager.getInstance().appRootMap.get( appId );
        if( appRoot != null && appRoot.getType() == AppIdType.ADMIN.value() ) {
            return true;
        }
        return false;
    }

    public static void createPermissionMap(List<AppDirRelation> appDirs, List<AppRelation> appRelations, Map<String,AppDirRelation> appDirRelationMap, Map<Long,List<Long>> appRelationMap,Map<Long,List<AppDirRelation>> appDirMap) {
        for( AppDirRelation appDirRelation : appDirs ){
            appDirRelationMap.put( appDirRelation.getDirId() + "-" + appDirRelation.getAppId(),appDirRelation );
            List<AppDirRelation> appDirRelations = appDirMap.get( appDirRelation.getAppId() );
            if( appDirRelations == null ){
                appDirRelations = new ArrayList<>();
                appDirMap.put( appDirRelation.getDirId(),appDirRelations );
            }
            appDirRelations.add( appDirRelation );
        }
        for( AppRelation appRelation : appRelations  ){
            if( VnnConfigUtil.isDebugOn() ){
                VnnLogUtil.log("appRelation => " + appRelation);
            }
            List<Long> parents = appRelationMap.get( appRelation.getSon() );
            if( parents == null ){
                parents = new LinkedList<>();

                appRelationMap.put( appRelation.getSon(),parents );
            }
            parents.add( appRelation.getParent() );
        }
    }

    public static void createRootMap(List<AppRoot> appRoots, Map<Long, AppRoot> appRootMap) {
        for( AppRoot appRoot : appRoots ){
            appRootMap.put( appRoot.getAppId(),appRoot );
        }
    }
}
