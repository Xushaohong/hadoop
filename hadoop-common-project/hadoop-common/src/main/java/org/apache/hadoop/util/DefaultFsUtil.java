package org.apache.hadoop.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.lang.reflect.Method;
import java.net.URI;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;

public class DefaultFsUtil {
  public static final Log LOG = LogFactory.getLog(DefaultFsUtil.class);

  public static final String RBF_REDIRECT_DEFAULTFS_ENABLE = "rbf.replace.defaultfs.enable";
  public static final String RBF_REDIRECT_DEFAULTFS_PATH = "rbf.replace.defaultfs.path";
  public static final String RBF_REDIRECT_DEFAULTFS_GROUP = "rbf.replace.defaultfs.group";
  public static final String TDW_UGI_GROUPNAME = "tdw.ugi.groupname";

  public static boolean rbfRedirectEnable(Configuration conf) {
    return conf.getBoolean(RBF_REDIRECT_DEFAULTFS_ENABLE, false);
  }

  public static void removeRbfRedirectConf(Configuration conf) {
    conf.set(RBF_REDIRECT_DEFAULTFS_ENABLE, null);
  }

  private static URI loadRbfDefaultFsUri(Configuration conf) {
    String groupName = conf.get(RBF_REDIRECT_DEFAULTFS_GROUP, conf.get(TDW_UGI_GROUPNAME));
    if (groupName == null || groupName.length() == 0) {
      LOG.warn("RBF redirect enabled but group not set.");
      return null;
    }
    String redirectPath = conf.get(RBF_REDIRECT_DEFAULTFS_PATH);
    if (redirectPath == null || redirectPath.isEmpty()) {
      LOG.warn("RBF redirect enabled but dir not set.");
      return null;
    }
    URI rbfDefaultFsUri = new Path(redirectPath, groupName).toUri();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Redirect uri: " + rbfDefaultFsUri);
    }
    return rbfDefaultFsUri;
  }

  private static String getRedirectedDefaultFsPath(URI redirectUri, Configuration conf) {
    try {
      FileSystem rbfFileSystem = FileSystem.get(redirectUri, conf);
      Method mGetRemotePath = rbfFileSystem.getClass().getMethod("getRemotePath", Path.class);
      Object res = mGetRemotePath.invoke(rbfFileSystem, new Path(redirectUri.getPath()));
      if (LOG.isDebugEnabled()) {
        LOG.debug("getRemotePath return: " + res);
      }
      return (String)res;
    }catch (Exception ex) {
      LOG.warn("Failed get redirected uri for " + redirectUri, ex);
    }
    return null;
  }

  /**
   * Replace old uri with a new uri from rbf
   * @param conf
   * @return
   */
  public static URI getRedirectedUri(Configuration conf) {
    URI rbfRedirectUri = loadRbfDefaultFsUri(conf);
    if (rbfRedirectUri != null) {
      String redirectedPath = getRedirectedDefaultFsPath(rbfRedirectUri, conf);
      if (redirectedPath != null && redirectedPath.length() > 0) {
        return URI.create(redirectedPath);
      }
    }
    return null;
  }

  private static String rebuildDefaultFs(URI uri) {
    StringBuilder defaultFs = new StringBuilder(uri.getScheme());
    defaultFs.append("://");
    defaultFs.append(uri.getAuthority());
    if(uri.getPort() > 0) {
      defaultFs.append(":");
      defaultFs.append(uri.getPort());
    }
    return defaultFs.toString();
  }

  public static boolean replaceDefaultFs(Configuration conf) {
    URI newDefaultFs = getRedirectedUri(conf);
    if(newDefaultFs != null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("replaceDefaultFs before defaultFs = " + conf.get(FS_DEFAULT_NAME_KEY));
      }
      conf.set(FS_DEFAULT_NAME_KEY, rebuildDefaultFs(newDefaultFs));
      if (LOG.isDebugEnabled()) {
        LOG.debug("replaceDefaultFs after defaultFs = " + conf.get(FS_DEFAULT_NAME_KEY));
      }
      return true;
    }
    return false;
  }
}
