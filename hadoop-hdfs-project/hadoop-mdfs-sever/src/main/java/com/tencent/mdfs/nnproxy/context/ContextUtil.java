package com.tencent.mdfs.nnproxy.context;

import static com.tencent.mdfs.config.Constants.DEBUG_KEY_CALLER;

import com.qq.cloud.taf.protocol.codec.jce.JceServiceRequest;
import com.tencent.mdfs.util.LocalCache;
import com.tencent.mdfs.util.VnnConfigUtil;
import com.tencent.mdfs.util.VnnLogUtil;
import com.tencent.mdfs.config.Constants;
import com.tencent.mdfs.config.MigDFSConfigKeys;
import java.util.HashMap;
import java.util.Map;


/**
 * create:chunxiaoli Date:2/6/18
 */
public class ContextUtil {

  //private static final Logger LOGGER = LogManager.getLogger("VnnLogUtil");




  public static boolean isOldSchema() {
    return Constants.MIG_DFS_OLD_SCHEMA.equals(
        getContextByKey(MigDFSConfigKeys.DFS_CLIENT_SCHEMA)
    );
  }

  public static String getClientInfo() {
    return getSchema() + "-" + getContextByKey(DEBUG_KEY_CALLER) + "-" + getContextTag();
  }

  public static String getCaller() {
    return getContextByKey(DEBUG_KEY_CALLER);
  }


  public static String getSchema() {
    return getContextByKey(MigDFSConfigKeys.DFS_CLIENT_SCHEMA);
  }

  public static String getUserName() {
    return getContextByKey(MigDFSConfigKeys.MIG_KEY_CLIENT_USER);
  }


  public static String getNameNodeUri() {
    String nnAddr = null;
    String clusterId = null;
    nnAddr = getContextByKey(MigDFSConfigKeys.MIG_KEY_CLIENT_NAME_NODE_URI);

    if (VnnConfigUtil.isDebugOn()) {
      //VnnLogUtil.log("default nnaddr:"+ nnAddr);
    }
    if (nnAddr != null) {
      clusterId = LocalCache.getClusterId(nnAddr);
    }
    if (clusterId != null && LocalCache.getNNAddr(clusterId) != null) {
      return LocalCache.getNNAddr(clusterId);
    }
    return nnAddr;
  }

  public static String getDefaultAddr() {
    return getNameNodeUri();
  }


  public static String getContextByKey(String key) {
    String value = "";

    try {
      JceServiceRequest req = getJceServiceRequest();
      if (req != null) {
        Map<String, String> context = req.getContext();

        if (context != null) {
          value = context.get(key);

        } else {
          VnnLogUtil.err("context is null!");
        }
      }
    } catch (Exception e) {
      VnnLogUtil.err("getContextByKey " + key + " ", e);
    }

    return value;
  }

  public static void updateSchema(String v) {
    updateContext(MigDFSConfigKeys.DFS_CLIENT_SCHEMA, v);
  }

  public static void updateContext(String k, String v) {
    JceServiceRequest req = getJceServiceRequest();
    if (req != null) {
      Map<String, String> context = req.getContext();
      if (context != null) {
        context.put(k, v);
      }
    }
  }



  public static String getContextTag() {
    String value = null;

    return value;
  }

  public static String getRemoteIp() {
    try {
      return "";//return getJSession().getRemoteIp();
    } catch (Exception e) {
      return null;
    }
  }


  public static JceServiceRequest getJceServiceRequest() {
    return null;
  }


  public static String getCurrentPath() {
    return getContextByKey(Constants.CLOUDHDFS_CLIENT_CURRENT_PATH);
  }

  public static String getCurrentWritingPath() {
    return getContextByKey(Constants.CLOUDHDFS_CLIENT_CURRENT_WRITING_PATH);
  }

}
