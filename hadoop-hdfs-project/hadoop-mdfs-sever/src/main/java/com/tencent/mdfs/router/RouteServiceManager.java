package com.tencent.mdfs.router;

import static com.tencent.mdfs.config.Constants.HDFS_DEFAULT_PATH;

import com.qq.cloud.taf.common.util.StringUtils;
import com.tencent.mdfs.entity.RouteInfo;
import com.tencent.mdfs.exception.MigHadoopException;
import com.tencent.mdfs.nnproxy.context.ContextUtil;
import com.tencent.mdfs.storage.VnnConfigServiceManager;
import com.tencent.mdfs.util.VnnConfigUtil;
import com.tencent.mdfs.util.VnnLogUtil;
import com.tencent.mdfs.util.VnnPathUtil;

/**
 * create:chunxiaoli
 * Date:2/7/18
 */
public class RouteServiceManager implements RouteService {


    private static RouteServiceManager instance;


    private RouteServiceManager() {

    }

    public synchronized static RouteServiceManager getInstance() {
        if (instance == null) {
            instance = new RouteServiceManager();
        }
        return instance;
    }

    @Override
    public RouteInfo route(final String fullPath) {

        if (fullPath == null) {
            return RouteUtil.getDefaultRouteInfo();
        }

        boolean isMDFS = VnnPathUtil.isMDFS(fullPath);

        String logicPath = VnnPathUtil.resolveLogicPath(fullPath, isMDFS);


        RouteInfo info = null;
        RouteInfo ret = null;

        if (!isMDFS) {
            info  = VnnPathUtil.parseSpecialPath(fullPath);
            if( info != null ){
                logicPath = info.getOriginLogicPath();
            }
            if( info == null ) {
                info = RouteUtil.getHDFSRouteInfo(logicPath, fullPath);
            }
        } else {
            try {
                info = VnnConfigServiceManager.getInstance().getRouteInfo(logicPath);
                if(info==null){
                    info  = VnnPathUtil.parseSpecialPath(fullPath);
                    if( info != null ){
                        logicPath = info.getOriginLogicPath();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                VnnLogUtil.err(" route for " + logicPath + " error " + e + " " + ContextUtil.getClientInfo());
            }
        }
        if (info != null) {


            String matchRealPath = info.realPath;

            final String finalRealPath = resolveRealPath( fullPath, logicPath, info);

            //VnnLogUtil.log("VnnConfigUtil.isDebugOn() " + VnnConfigUtil.isDebugOn());

            VnnLogUtil.debug(ContextUtil.getClientInfo() + " route path: " + logicPath + " =>find (" +
                    info.logicPath + " ==> " + matchRealPath + " )==>final  hdfs://" + info.addr + finalRealPath);

            ret = new RouteInfo(info.id,logicPath, finalRealPath, info.addr, info.finalPath, fullPath, info.getClusterId());
            ret.setMatchType(info.getMatchType());
            ret.setClusterInfos(info.getClusterInfos());


        } else {
            VnnLogUtil.log(ContextUtil.getClientInfo() + " something wrong can not route for:" + logicPath);
        }

        return ret;


    }

    //fixme
    private String resolveRealPath(final String fullPath, final String logicPath, final RouteInfo info) {
        if( info.getMatchType() == RouteInfo.MATCH_TYPE_HDFS ){
            return info.realPath;
        }

        String ret = null;

        //subdir
        if (info.getMatchType() == RouteInfo.MATCH_TYPE_SUBDIR) {
            ret = logicPath.replaceAll(info.logicPath, info.realPath)
                    .replaceAll("//", "/");
        } else if (info.getMatchType() == RouteInfo.MATCH_TYPE_COMPLETE  ) {
            if (StringUtils.isEmpty(info.realPath)) {
                ret = "/";
            } else {
                ret = info.realPath;
            }

        }
        //final path is empty throw ex
        if (StringUtils.isEmpty(ret)) {

            VnnLogUtil.err("can not get final real path for " + logicPath + " " + info + " " + ContextUtil.getClientInfo());

            throw new MigHadoopException("can not get final real path for " + logicPath + " " + ContextUtil.getClientInfo());
        }

        //fix trash path
        ret = VnnPathUtil.concatTrashPath(fullPath, ret);

        if (VnnConfigUtil.isDebugOn()) {
            VnnLogUtil.debug(" resolveRealPath full=>" + fullPath + " logicPath==>"
                    + logicPath + " find map ret=> " + info + " final ret=>" + ret);
        }

        return ret;
    }


    public String getDefaultPath(String dir) {
        return HDFS_DEFAULT_PATH + dir;
    }
}
