package com.tencent.mdfs.util;


import static com.tencent.mdfs.entity.RouteInfo.MATCH_TYPE_COMPLETE;

import com.tencent.mdfs.entity.RouteInfo;
import com.tencent.mdfs.exception.MigHadoopException;
import com.tencent.mdfs.nnproxy.context.ContextUtil;
import com.tencent.mdfs.provider.pojo.SpecialPath;
import com.tencent.mdfs.router.RouteUtil;
import com.tencent.mdfs.storage.VnnConfigServiceManager;
import com.tencent.mdfs.config.Constants;
import java.net.URI;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.ipc.Server;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;

/**
 * create:chunxiaoli
 * Date:5/22/18
 */
public class VnnPathUtil {

    private static final Logger LOGGER = LogManager.getLogger("path");

    public final static String COLON = ":";
    public final static String HASH_TAG = "#";
    public final static String QUERY_TAG = "?";
    public final static String HASH_ENCODE_TAG = "%23";
    public final static String QUERY_ENCODE_TAG = "%3F";
    public final static String COLON_ENCODE_TAG = "%3A";
    public final static String URL_ENCODE_TAG = "%25";

    public final static String AUTHOR_PORT_TAG_REG = "\\w+://(\\w)|([0-255]\\.[0-255]\\.[0-255]\\.[0-255])[:\\d+]*";

    public final static String TRASH_PATH_CURRENT = "/user/mqq/.Trash/Current";
    public final static String TRASH_PATH = "/user/mqq/.Trash";
    //do some special handle for hive /tmp/hive/staging
    public final static  String hiveStagingPre = "mdfs://cloudhdfs/tmp";

    private static final String IPADDRESS_PATTERN =
            "^([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
                    "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
                    "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
                    "([01]?\\d\\d?|2[0-4]\\d|25[0-5])$";

    private static final Pattern PatternIP = Pattern.compile(IPADDRESS_PATTERN);

    private final static Pattern IP_PORT = Pattern.compile("([1-9]|[1-9]\\\\d|1\\\\d{2}|2[0-4]\\\\d|25[0-5])(\\\\.(\\\\d|[1-9]\\\\d|1\\\\d{2}|2[0-4]\\\\d|25[0-5])){3}");

    public static boolean isMDFS(String path) {
        return path!=null&&path.startsWith("/cloudhdfs");
    }

    public static String resolveLogicPath(String file) {
        return resolveLogicPath(file, isMDFS(file));
    }


    //todo add cache
    //trim /user/mqq/.Trash/Current to empty
    public static String resolveLogicPath(final String file, boolean isMdfs) {
        String result = null;

        if (file == null) {
            return null;
        }

        if ( !isMdfs ) {
            result = new Path(file).toUri().getPath();
        }else {
            result = file.replace(Constants.DFS_SCHEMA_MDFS, "/")
                    .replace(TRASH_PATH_CURRENT, "").replace(TRASH_PATH, "");
        }


        int queryIndex = result.indexOf(QUERY_TAG);

        if(queryIndex>0){
            result = result.substring(0,queryIndex);
        }

        int hashIndex = result.indexOf(HASH_TAG);

        if(hashIndex>0){
            result = result.substring(0,hashIndex);
        }

        int queryIdx = result.indexOf(QUERY_ENCODE_TAG);

        if (queryIdx > 0) {
            result = result.substring(0, queryIdx);
        }
        int hashIdx = result.indexOf(HASH_ENCODE_TAG);

        if (hashIdx > 0) {
            result = result.substring(0, hashIdx);
        }

        if(result.contains(URL_ENCODE_TAG)){
            result = result.replaceAll(URL_ENCODE_TAG,"%");
        }


        LOGGER.info("resolveLogicPath " + file + "==>" + result + " from " + Server
                .getRemoteAddress() );



        return result;
    }


    public static String getPathName(String file) {
        String result;

        if (file == null) {
            return null;
        }

        if (!file.startsWith(Constants.DFS_SCHEMA_MDFS)) {
            URI uri = URI.create(file);
            result = uri.getPath();
        } else {
            result = file.replace(Constants.DFS_SCHEMA_MDFS, "/");
        }

        if (!StringUtils.isEmpty(result) && !DFSUtil.isValidName(result)) {
            throw new IllegalArgumentException("Pathname " + result + " from " +
                    file + " is not a  valid MDFS filename.");
        }
        return result;
    }

    public static String getNNAddr(String path) {

        String nnAddr = null;

        if (isMDFS(path)) {
            throw new MigHadoopException("can not path nn addr for " + path);
        }

        URI uri = URI.create(path);

        if (uri.isAbsolute()) {

            String nn = uri.getAuthority();

            if (VnnConfigUtil.isDebugOn()) {
                VnnLogUtil.log("getNNAddr " + path + " uri: " + uri + " nn:" + nn);
            }


            if (isValidIP(nn)) {
                nnAddr = nn;
                String clusterId = LocalCache.getClusterId(nnAddr);
                if( clusterId != null ){
                    String activeNn = LocalCache.getNNAddr( clusterId );
                    if( activeNn != null ){
                        nnAddr = activeNn;
                    }
                }
            } else {

                VnnLogUtil.log("invalid nn "+nn+" "+path);

                nnAddr = ContextUtil.getNameNodeUri();
            }
        } else {
            nnAddr = ContextUtil.getNameNodeUri();
        }


        if (VnnConfigUtil.isDebugOn()) {
            VnnLogUtil.debug("final nn for  " + path + " ======> " + nnAddr);
        }

        if(nnAddr==null){
            VnnLogUtil.err("can not get nn addr "+path);
            throw new MigHadoopException("can not path nn addr for " + path);
        }


        return nnAddr;


    }

    //fixme
    public static boolean isValidIP(String nn) {
        if(Strings.isEmpty(nn)){
            return false;
        }
        String[] parts = nn.split(":");
        return parts.length > 0 && isIp(parts[0]);
    }

    public static boolean isIp(String ip){
        return PatternIP.matcher(ip).matches();
    }

    public static boolean isCompleteMatch(RouteInfo info) {
        return info.getMatchType() == MATCH_TYPE_COMPLETE;
    }

    public static boolean isTrashPath(String path) {
        return path.contains(TRASH_PATH);
    }

    public static String concatTrashPath(String fullPath, String path) {

        return isTrashPath(fullPath) ? TRASH_PATH_CURRENT + path : path;
    }

    public static RouteInfo parseSpecialPath(String fullPath){
        RouteInfo ret = null;
        try {
            for (Map.Entry<String, SpecialPath> entity : LocalCache.specialPathMap.entrySet()) {
                if (entity.getKey() != null && fullPath.startsWith(entity.getKey())) {

                    String newFullPath = entity.getValue().getReplacePathPre() + fullPath.substring(entity.getKey().length());
                    VnnLogUtil.log(" fullPath " + fullPath + " change to " + newFullPath + ",db:" + entity.getValue());
                    final String newLogicPath = VnnPathUtil.resolveLogicPath(newFullPath, entity.getValue().isMdfs());

                    if( entity.getValue().isMdfs() ){
                        RouteInfo routeInfo =  VnnConfigServiceManager.getInstance().getRouteInfo( newLogicPath );
                        routeInfo.setOriginLogicPath( newLogicPath );
                        return routeInfo;
                    }
                    ret = RouteUtil.getHDFSRouteInfo(newLogicPath, newFullPath);
                    ret.setOriginLogicPath( newLogicPath );
                    if (VnnConfigUtil.isDebugOn()) {
                        VnnLogUtil.log(fullPath + " " + newFullPath + " " + newFullPath + " " + ret);
                    }
                    return ret;
                }
            }
        }catch (Exception e){
            VnnLogUtil.warn("parseSpecialPath failed"+e);
        }
        return ret;
    }

    public static boolean isProtectDir(final  String dir){

        boolean protectLevel = isProtectDirLevel(dir);

        return VnnConfigUtil.getProtectDirs().contains(dir)||protectLevel;


    }

    public static boolean isProtectDirLevel(final String dir){

        if(protectDirLevelOn()){
            String[] arr = dir.split("/");
            return arr.length <= VnnConfigUtil.getProtectDirLevel();
        }

        return false;


    }

    private static boolean protectDirLevelOn() {
        return VnnConfigUtil.protectDirLevelOn();
    }
}
