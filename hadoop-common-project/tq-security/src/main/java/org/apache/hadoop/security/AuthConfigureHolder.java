package org.apache.hadoop.security;

import com.google.common.collect.Sets;
import com.tencent.tdw.security.utils.StringUtils;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthConfigureHolder {

  private static final Logger LOG = LoggerFactory.getLogger(AuthConfigureHolder.class);
  private static String AUTH_CONF_FILE_NAME = "auth-config.properties";
  private static String AUTH_ENABLE_KEY = "auth.enable";
  private static String AUTH_SMK_ENABLE_KEY = "auth.smk.enable";
  private static String AUTHORIZATION_ENABLE_KEY = "authorization.enable";
  private static String UNION_TOKEN_ENABLE_KEY = "union.token.enable";
  private static String TQ_TOKEN_ENABLE_KEY = "tq.token.enable";
  private static String AUTH_WEB_CONFIG_ENABLE_KEY = "auth.web.config.enable";
  private static String AUTH_PROTOCOL_CONFIG_KEY = "auth.protocol.list";
  private static String PLAIN_PROTOCOL_CONFIG_KEY = "plain.protocol.list";
  private static String ALLOW_USER_LIST_KEY = "auth.allow.user.list";
  private static String TAUTH_SASL_VERSION_KEY = "tauth.sasl.version";
  private static String REGULAR_USERNAME_ENABLE_KEY = "tq.regular.username.enable";
  private static String REGULAR_USER_UNIFY_PERMISSION_ENABLE_KEY =
      "tq.regular.user.permission.unify.enable";

  // config for union token, default is 20 ms
  private static String UNION_TOKEN_ASYNC_INTERVAL_KEY = "union.token.async.interval";
  private static String UNION_TOKEN_ASYNC_CAPACITY_KEY = "union.token.async.capacity";
  private static String UNION_TOKEN_ASYNC_BATCH_KEY = "union.token.async.batch";
  private static String UNION_TOKEN_WORKER_ID_KEY = "union.token.worker.id";

  private static String UNION_TOKEN_MAX_GETTING_REQUEST_KEY = "union.token.max.getting.request";

  private static boolean AUTH_WEB_CONFIG_ENABLE = false;
  private static boolean AUTH_ENABLE = true;
  private static boolean AUTH_SMK_ENABLE = true;
  private static boolean AUTHORIZATION_ENABLE = true;
  private static boolean UNION_TOKEN_ENABLE = true;
  private static boolean TQ_TOKEN_ENABLE = true;
  private static Set<String> ALLOW_USER_LIST = new HashSet<>();
  private static final ProtocolPolicyManagement PROTOCOL_POLICY_MANAGEMENT = new
      ProtocolPolicyManagement();
  private static int TAUTH_SASL_VERSION = 1;
  private static boolean REGULAR_USERNAME_ENABLE = false;
  private final static String IRREGULAR_USER_PREFIX = "tdw_";
  private static boolean REGULAR_USER_UNIFY_PERMISSION_ENABLE = true;

  private static long UNION_TOKEN_ASYNC_INTERVAL = 20L;
  private static int UNION_TOKEN_ASYNC_CAPACITY = 10240;
  private static int UNION_TOKEN_ASYNC_BATCH = 100;
  private static int UNION_TOKEN_WORKER_ID = -1;

  private static int UNION_TOKEN_MAX_GETTING_REQUEST = 512;

  static {
    refreshAuthConfig();
  }


  public static boolean refreshAuthConfig() {
    Properties authProps = new Properties();
    try (InputStream input = ClassLoader.getSystemResourceAsStream(AUTH_CONF_FILE_NAME)) {
      if (input == null) {
        return false;
      }
      authProps.load(input);
    } catch (Exception e) {
      LOG.warn("Refresh auth config failed: " + e.getMessage());
    }
    AUTH_WEB_CONFIG_ENABLE = Boolean.parseBoolean(Utils.getProperty(authProps, AUTH_WEB_CONFIG_ENABLE_KEY, "false"));
    AUTH_ENABLE = Boolean.parseBoolean(Utils.getProperty(authProps, AUTH_ENABLE_KEY, "true"));
    AUTH_SMK_ENABLE = Boolean.parseBoolean(Utils.getProperty(authProps, AUTH_SMK_ENABLE_KEY, "true"));
    AUTHORIZATION_ENABLE = Boolean.parseBoolean(Utils.getProperty(authProps, AUTHORIZATION_ENABLE_KEY, "true"));
    UNION_TOKEN_ENABLE = Boolean.parseBoolean(Utils.getProperty(authProps, UNION_TOKEN_ENABLE_KEY, "true"));
    TQ_TOKEN_ENABLE = Boolean.parseBoolean(Utils.getProperty(authProps, TQ_TOKEN_ENABLE_KEY, "true"));
    PROTOCOL_POLICY_MANAGEMENT.setAuthProtocols(Utils.getProperty(authProps, AUTH_PROTOCOL_CONFIG_KEY, ""));
    PROTOCOL_POLICY_MANAGEMENT.setPlainProtocols(Utils.getProperty(authProps, PLAIN_PROTOCOL_CONFIG_KEY, ""));
    TAUTH_SASL_VERSION = Integer.parseInt(Utils.getProperty(authProps, TAUTH_SASL_VERSION_KEY, "1"));
    REGULAR_USERNAME_ENABLE = Boolean.parseBoolean(Utils.getProperty(authProps, REGULAR_USERNAME_ENABLE_KEY, "false"));
    REGULAR_USER_UNIFY_PERMISSION_ENABLE = Boolean.parseBoolean(Utils.getProperty(authProps, REGULAR_USER_UNIFY_PERMISSION_ENABLE_KEY, "true"));
    AuthConfigureHolder.setAllowUserList(Utils.getProperty(authProps, ALLOW_USER_LIST_KEY, ""));
    refreshUnionTokenConfig(authProps);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Refresh auth config successfully:" + toDigest());
    }
    return true;
  }

  private static void refreshUnionTokenConfig(Properties authProps) {
    long unionAsyncInterval = Long.parseLong(Utils.getProperty(authProps, UNION_TOKEN_ASYNC_INTERVAL_KEY, "20"));
    if (unionAsyncInterval <= 0) {
      LOG.error("{} is not allowed to set negative", UNION_TOKEN_ASYNC_INTERVAL_KEY);
    } else {
      UNION_TOKEN_ASYNC_INTERVAL = unionAsyncInterval;
    }
    UNION_TOKEN_ASYNC_CAPACITY = Integer.parseInt(Utils.getProperty(authProps, UNION_TOKEN_ASYNC_CAPACITY_KEY, "10240"));
    UNION_TOKEN_WORKER_ID = Integer.parseInt(Utils.getProperty(authProps, UNION_TOKEN_WORKER_ID_KEY, "-1"));
    UNION_TOKEN_ASYNC_BATCH = Integer.parseInt(Utils.getProperty(authProps, UNION_TOKEN_ASYNC_BATCH_KEY, "100"));
    UNION_TOKEN_MAX_GETTING_REQUEST = Integer.parseInt(Utils.getProperty(authProps, UNION_TOKEN_MAX_GETTING_REQUEST_KEY, "512"));
  }

  public static void setAuthEnable(boolean authEnable) {
    AUTH_ENABLE = authEnable;
  }
  public static void setAuthorizationEnable(boolean authorizationEnable) {
    AUTHORIZATION_ENABLE = authorizationEnable;
  }

  public static void setUnionTokenEnable(boolean unionTokenEnable) {
    UNION_TOKEN_ENABLE = unionTokenEnable;
  }

  public static void setTqTokenEnable(boolean tqTokenEnable) {
    TQ_TOKEN_ENABLE = tqTokenEnable;
  }

  public static void setAllowUserList(String allowUserList) {
    ALLOW_USER_LIST = StringUtils.isBlank(allowUserList)
        ? Collections.<String>emptySet()
        : Sets.newHashSet(StringUtils.split(allowUserList));
  }

  public static boolean isNotAllow(String userName) {
    return !ALLOW_USER_LIST.isEmpty() && !ALLOW_USER_LIST.contains(userName);
  }

  public static boolean isAuthEnable() {
    return AUTH_ENABLE;
  }

  public static boolean isAuthSmkEnable() {
    return AUTH_SMK_ENABLE;
  }

  public static boolean isAuthorizationEnable() {
    return AUTHORIZATION_ENABLE;
  }

  public static boolean isUnionTokenEnable() {
    return UNION_TOKEN_ENABLE;
  }

  public static boolean isTqTokenEnable() {
    return TQ_TOKEN_ENABLE;
  }

  public static int getTAuthSaslVersion() {
    return TAUTH_SASL_VERSION;
  }

  public static boolean isRegularUsernameEnable() {
    return REGULAR_USERNAME_ENABLE;
  }

  public static boolean isRegularUserUnifyPermissionEnable() {
    return REGULAR_USER_UNIFY_PERMISSION_ENABLE;
  }

  public static String regularUserName(String userName) {
    if (userName != null && userName.startsWith(IRREGULAR_USER_PREFIX)) {
      userName = userName.substring(IRREGULAR_USER_PREFIX.length());
    }
    return userName;
  }


  public static String toDigest() {
    StringBuilder builder = new StringBuilder();
    builder.append("web config enable:")
        .append(AUTH_WEB_CONFIG_ENABLE)
        .append("\n")
        .append("auth enable:")
        .append(AUTH_ENABLE)
        .append("\n")
        .append("authorization enable:")
        .append(AUTHORIZATION_ENABLE)
        .append("\n")
        .append("union token enable:")
        .append(UNION_TOKEN_ENABLE)
        .append("\n")
        .append("tq token enable:")
        .append(TQ_TOKEN_ENABLE)
        .append("\n")
        .append("protocol policy:")
        .append(PROTOCOL_POLICY_MANAGEMENT)
        .append("\n")
        .append("tauth sasl version:")
        .append(TAUTH_SASL_VERSION)
        .append("\n")
        .append("regular username enable:")
        .append(REGULAR_USERNAME_ENABLE)
        .append("\n")
        .append("regular user unify permission enable:")
        .append(REGULAR_USER_UNIFY_PERMISSION_ENABLE)
        .append("\n")
        .append("irregular name prefix:")
        .append(IRREGULAR_USER_PREFIX)
        .append("\n")
        .append("load by " + AUTH_CONF_FILE_NAME)
        .append("\n");
    return builder.toString();
  }

  public static ProtocolPolicyManagement getProtocolPolicyManagement() {
    return PROTOCOL_POLICY_MANAGEMENT;
  }

  public static long getUnionTokenAsyncInterval() {
    return UNION_TOKEN_ASYNC_INTERVAL;
  }

  public static int getUnionTokenAsyncCapacity() {
    return UNION_TOKEN_ASYNC_CAPACITY;
  }

  public static int getUnionTokenMaxGettingRequest() {
    return UNION_TOKEN_MAX_GETTING_REQUEST;
  }

  public static int getUnionTokenWorkerId() {
    return UNION_TOKEN_WORKER_ID;
  }

  public static int getUnionTokenAsyncBatch() {
    return UNION_TOKEN_ASYNC_BATCH;
  }

  public static class ProtocolPolicyManagement {

    private Set<String> authProtocols = new HashSet<>();
    private Set<String> plainProtocols = new HashSet<>();


    public ProtocolPolicyManagement() {
      this("", "");
    }

    public ProtocolPolicyManagement(String authProtocols, String plainProtocols) {
      if (!Utils.isNullOrEmpty(authProtocols)) {
        this.authProtocols.addAll(StringUtils.getTrimmedStrings(
            Arrays.asList(StringUtils.split(authProtocols))));
      }

      if (!Utils.isNullOrEmpty(plainProtocols)) {
        this.plainProtocols.addAll(StringUtils.getTrimmedStrings(
            Arrays.asList(StringUtils.split(plainProtocols))));
      }
    }

    public void setAuthProtocols(String authProtocols) {
      Set<String> authProtocolSet = new HashSet<>();
      if (!Utils.isNullOrEmpty(authProtocols)) {
        authProtocolSet.addAll(StringUtils.getTrimmedStrings(
            Arrays.asList(StringUtils.split(authProtocols))));
      }
      this.authProtocols = authProtocolSet;
    }

    public void setPlainProtocols(String plainProtocols) {
      Set<String> plainProtocolSet = new HashSet<>();
      if (!Utils.isNullOrEmpty(plainProtocols)) {
        plainProtocolSet.addAll(StringUtils.getTrimmedStrings(
            Arrays.asList(StringUtils.split(plainProtocols))));
      }
      this.plainProtocols = plainProtocolSet;
    }


    public boolean isNeedAuth(String protocol) {

      if (!plainProtocols.isEmpty()) {
        return !plainProtocols.contains(protocol);
      }

      if (!authProtocols.isEmpty()) {
        return authProtocols.contains(protocol);
      }

      return true;
    }

    @Override
    public String toString() {
      return "ProtocolPolicyManagement{" +
          "authProtocols=" + authProtocols +
          ", plainProtocols=" + plainProtocols +
          '}';
    }
  }

  public static class RefreshAuthServlet extends HttpServlet {


    protected boolean isAccessAllowed(HttpServletRequest request, HttpServletResponse response) throws IOException {
      return true;
    }

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response)
        throws IOException {
      if (!isAccessAllowed(request, response)) {
        return;
      }

      response.setContentType("text/plain; charset=UTF-8");
      StringBuilder digestBuilder = new StringBuilder();
      digestBuilder.append("Before:").append(AuthConfigureHolder.toDigest());
      try {
        AuthConfigureHolder.refreshAuthConfig();
        if (AUTH_WEB_CONFIG_ENABLE) {
          String authEnable = request.getParameter(AUTH_ENABLE_KEY);
          String authorizationEnable = request.getParameter(AUTHORIZATION_ENABLE_KEY);
          String unionTokenEnable = request.getParameter(UNION_TOKEN_ENABLE_KEY);
          String tqTokenEnable = request.getParameter(TQ_TOKEN_ENABLE_KEY);
          String allowUserList = request.getParameter(ALLOW_USER_LIST_KEY);
          String authProtocols = request.getParameter(AUTH_PROTOCOL_CONFIG_KEY);
          String plainProtocols = request.getParameter(PLAIN_PROTOCOL_CONFIG_KEY);
          if (Utils.isNotNullOrEmpty(authEnable)) {
            AuthConfigureHolder.setAuthEnable(Boolean.parseBoolean(authEnable));
          }
          if(Utils.isNotNullOrEmpty(authorizationEnable)) {
            AuthConfigureHolder.setAuthorizationEnable(Boolean.parseBoolean(authorizationEnable));
          }
          if (Utils.isNotNullOrEmpty(unionTokenEnable)) {
            AuthConfigureHolder.setUnionTokenEnable(Boolean.parseBoolean(unionTokenEnable));
          }
          if (Utils.isNotNullOrEmpty(tqTokenEnable)) {
            AuthConfigureHolder.setTqTokenEnable(Boolean.parseBoolean(tqTokenEnable));
          }
          if (Utils.isNotNullOrEmpty(authProtocols)) {
            AuthConfigureHolder.getProtocolPolicyManagement().setAuthProtocols(authProtocols);
          }
          if (Utils.isNotNullOrEmpty(plainProtocols)) {
            AuthConfigureHolder.getProtocolPolicyManagement().setPlainProtocols(plainProtocols);
            AuthConfigureHolder.getProtocolPolicyManagement().setPlainProtocols(plainProtocols);
          }

          if (Utils.isNotNullOrEmpty(allowUserList)) {
            AuthConfigureHolder.setAllowUserList(allowUserList);
          }
        }
      } catch (Exception e) {
        digestBuilder.append("\n");
        digestBuilder.append("Failed:").append(e.getMessage());
      }
      digestBuilder.append("\n");
      digestBuilder.append("----------------------------------------------------------------------------------------");
      digestBuilder.append("\n");
      digestBuilder.append("After:").append(AuthConfigureHolder.toDigest());

      if (!AUTH_WEB_CONFIG_ENABLE) {
        digestBuilder.append("\n");
        digestBuilder.append("And Web Config is disable");
      }
      response.getWriter().write(digestBuilder.toString());
      LOG.info(digestBuilder.toString());
    }
  }
}
