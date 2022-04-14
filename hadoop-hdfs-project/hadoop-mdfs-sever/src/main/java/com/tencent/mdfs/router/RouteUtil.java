package com.tencent.mdfs.router;

import static com.tencent.mdfs.entity.RouteInfo.MATCH_TYPE_DEFAULT_NN;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.qq.cloud.taf.common.util.StringUtils;
import com.tencent.mdfs.entity.RouteInfo;
import com.tencent.mdfs.exception.MigHadoopException;
import com.tencent.mdfs.nnproxy.context.ContextUtil;
import com.tencent.mdfs.provider.pojo.DirInfo;
import com.tencent.mdfs.storage.VnnConfigServiceManager;
import com.tencent.mdfs.util.LocalCache;
import com.tencent.mdfs.util.VnnConfigUtil;
import com.tencent.mdfs.util.VnnLogUtil;
import com.tencent.mdfs.util.VnnPathUtil;
import com.tencent.mdfs.util.PathUtil;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * create:chunxiaoli
 * Date:5/15/18
 */
public class RouteUtil {

    public static final String DEFAUTL_PATH_FOR_NON_PATH_METHOD="mdfs://cloudhdfs/default";

    public static RouteInfo getHDFSRouteInfo(String logicPath,String path){

        if(VnnConfigUtil.isDebugOn()){

        }
        if (StringUtils.isEmpty( logicPath )) {
            logicPath = "/";
        }
        RouteInfo info=  new RouteInfo(null,logicPath, logicPath, VnnPathUtil.getNNAddr(path),
                null,null,LocalCache.getClusterId(VnnPathUtil.getNNAddr(path)));
        info.setMatchType(RouteInfo.MATCH_TYPE_HDFS);
        return info;
    }

    public static RouteInfo getDefaultRouteInfo(String logicPath,String path){
        RouteInfo info=  new RouteInfo(null,logicPath, logicPath,
                VnnPathUtil.getNNAddr(path),null,null,null);
        info.setMatchType(MATCH_TYPE_DEFAULT_NN);
        return info;
    }

    public static RouteInfo getDefaultRouteInfo(){

        String addr = ContextUtil.getDefaultAddr();

        if(!StringUtils.isEmpty(addr)){

            //VnnLogUtil.debug("no route info found in config service use default pass by client ");

            RouteInfo info= new RouteInfo(null,null,null,addr,null,null,null);
            info.setMatchType(MATCH_TYPE_DEFAULT_NN);
            return info;

        }else {
            VnnLogUtil.err("getDefaultRouteInfo error ,can not get default nn addr :"+ContextUtil.getClientInfo());
            throw new MigHadoopException("getDefaultRouteInfo error ,can not get default nn addr :"+ContextUtil.getClientInfo());
        }
    }

    public static RouteInfo getRouteInfoForMethodWithouPath(){

        RouteInfo info = RouteServiceManager.getInstance().route(DEFAUTL_PATH_FOR_NON_PATH_METHOD);
        return info;
    }

    public static RouteInfo getMatchRouteInfo(Map<String, RouteInfo> dirPath2RouteMap, int maxDirLevel, String dir) {



        String[] parts = dir.split(File.separator);

        List<String> list = Arrays.asList(parts);

        RouteInfo ret;

        int lev = (maxDirLevel > 0 && parts.length > maxDirLevel) ? maxDirLevel + 1 : parts.length;

        do {
            List<String> sub = list.subList(0, lev);
            String d = PathUtil.join(sub);
            try {
                VnnConfigServiceManager.readWriteLock.readLock().lock();
                ret = dirPath2RouteMap.get(d);
            }finally {
                VnnConfigServiceManager.readWriteLock.readLock().unlock();
            }
            if (ret != null) {
                //System.out.println("find " + ret);
                break;
            }
        } while (lev-- > 0);

        if(ret!=null){
            ret= new RouteInfo(ret.id,ret.logicPath,ret.realPath,ret.addr,ret.finalPath,ret.originPath,ret.getClusterId());
            ret.setMatchType(RouteInfo.MATCH_TYPE_SUBDIR);
        }

        return ret ;

    }
    public static JavaType getCollectionType(Class<?> collectionClass,ObjectMapper objectMapper, Class<?>... elementClasses) {
        return objectMapper.getTypeFactory().constructParametricType(collectionClass, elementClasses);
    }

    public static RouteInfo convert(DirInfo dir){

        RouteInfo routeInfo = null;
        try{
            routeInfo = new RouteInfo(dir.getDirId(),dir.getLogicPath(),
                    dir.getRealPath(), dir.getNamenodeAddr(),null,null,dir.getClusterId());

            /*if(!StringUtil.isEmpty(dir.getClusterInfos())){
                ObjectMapper mapper = new ObjectMapper();
                JavaType javaType = getCollectionType(List.class,mapper, RouteInfoPair.class);
                List<RouteInfoPair> pairs =  mapper.readValue(dir.getClusterInfos(), javaType);
                    routeInfo.setClusterInfos(pairs);
            }*/
        }catch (Exception e){
            e.printStackTrace();
            VnnLogUtil.err("convert  dir "+dir+" error "+e);
        }


        return routeInfo;
    }
}
