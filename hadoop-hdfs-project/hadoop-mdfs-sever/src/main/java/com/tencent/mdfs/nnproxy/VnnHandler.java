package com.tencent.mdfs.nnproxy;

import com.tencent.mdfs.nnproxy.context.RpcContextUtils;
import com.tencent.mdfs.util.JsonUtils;
import com.tencent.mdfs.util.MonitorUtils;
import com.tencent.mdfs.util.VnnHa;
import com.tencent.mdfs.util.VnnLogUtil;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class VnnHandler implements InvocationHandler {

  private static final Logger auditLog = LogManager.getLogger("audit");


  private final Object target;

  public VnnHandler(Object target){
    this.target = target;
  }

  private String getUserName() {
    UserGroupInformation remoteUser = Server.getRemoteUser();
    String userName = remoteUser.getUserName();
    return userName;
  }

  public String getRealUserName() {
    UserGroupInformation remoteUser = Server.getRemoteUser();
    UserGroupInformation realUserInfo = remoteUser.getRealUser();
    String realUser = null;
    if (null != realUserInfo) {
      realUser = realUserInfo.getUserName();
    }
    if (StringUtils.isBlank(realUser)) {
      realUser = "none";
    }
    return realUser;
  }

  public String getAuthMethod() {
    String authMethod = null;
    UserGroupInformation remoteUser = Server.getRemoteUser();
    if (null != remoteUser.getRealAuthenticationMethod()) {
      authMethod = remoteUser.getRealAuthenticationMethod().name();
    }
    if (StringUtils.isBlank(authMethod)) {
      authMethod = "none";
    }
    return authMethod;
  }

  public String getRemoteAddress() {
    return Server.getRemoteAddress();
  }

  private Map<String, Object> getCurrentCallInfo() {
    int callId = Server.getCallId();
    int callRetryCount = Server.getCallRetryCount();
    byte[] clientId = Server.getClientId();
    String clientIdStr = "";
    if (null != clientId) {
      clientIdStr = Hex.encodeHexString(clientId);
    }
    String protocol = Server.getProtocol();


    String detailsMsg = Server.getCurCall().get().toString();


    Map<String, Object> parameters = new HashMap<>();
    parameters.put("callId", String.valueOf(callId));
    parameters.put("callRetryCount", String.valueOf(callRetryCount));
    parameters.put("clientIdStr", clientIdStr);
    parameters.put("protocol", protocol);
    parameters.put("remoteAddress", getRemoteAddress());
    parameters.put("userName", getUserName());
    parameters.put("realUser", getRealUserName());
    parameters.put("detailsMsg", detailsMsg);
    parameters.put("authMethod", getAuthMethod());
    return parameters;
  }

  private Map<String, Object> buildMethodParameters(Method method, Object[] args) {
    String name = method.getName();
    Map<String, Object> methodParameters = new HashMap<>();
    Class<?>[] parameterTypes = method.getParameterTypes();
    if (parameterTypes == null && parameterTypes.length <= 0) {
      return methodParameters;
    }
    for (int i = 0; i < parameterTypes.length; i++) {
      methodParameters.put(String.valueOf(i), args[i]);
    }
    return methodParameters;
  }
  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    VnnLogUtil.debug("before invoke:"+method.getName());
    auditLog.info("test:"+method.getName());
    long s = System.currentTimeMillis();
    Map<String, Object> methodParameters =  null;
    long buildMethodParamtersElt = 0L;
    boolean optRet = false;
    try {
      long s1 = System.currentTimeMillis();
      methodParameters = buildMethodParameters(method, args);
      long e1 = System.currentTimeMillis();
      buildMethodParamtersElt = e1 - s1;
      Object result = method.invoke(target, args);  // 调用 target 的 method 方法
      optRet = true;
      return result;
    }catch (InvocationTargetException t) {
      VnnLogUtil.debug("after invoke failed:" + method.getName() );
        Throwable cause = t.getCause();
        if(VnnHa.refreshAddr(cause)){
          try {
            return method.invoke(target, args);
          }catch (Throwable e){
            VnnLogUtil.debug("after refreshAddr invoke failed again:" + method.getName() + e );
          }
        }
        if (cause != null) {
          VnnLogUtil.debug("throw excetion," + method.getName() + ":" + cause  );
          throw  cause;
        }
      throw t;
    } finally {
      long e = System.currentTimeMillis();
      long elt = e - s;
      Map<String, String> commonKeys = MonitorUtils.getCommonKeys();
      Map<String, String> keys = new LinkedHashMap<>();
      keys.putAll(commonKeys);
      keys.put("remoteAddr", getRemoteAddress());//Caller (hdfs client ip)
      keys.put("nnIp", RpcContextUtils.getRealNNIp());//namenode ip
      keys.put("protocol", Server.getProtocol());//protocol type
      keys.put("rpcMethodName", method.getName());//rpc method name

      //call user, If it is executed by mqq proxy user, here is the proxy user
      keys.put("user", getUserName());
      //real user，If it is executed by mqq proxy user, here is the proxy user
      keys.put("realUser", getRealUserName());
      keys.put("authMethod", getAuthMethod());//Authentication method
      MonitorUtils.reportVnnServer(keys, optRet, s);
      Map<String, Object> currentCallInfo = getCurrentCallInfo();
      String timeStr = DateFormatUtils.format(new Date(s), "yyyy-MM-dd HH:mm:ss");
      currentCallInfo.put("startTimestamp", s);
      currentCallInfo.put("endTimestamp", e);
      currentCallInfo.put("elt", elt);
      currentCallInfo.put("buildMethodParamtersElt", buildMethodParamtersElt);
      currentCallInfo.put("methodParameters", methodParameters);
      currentCallInfo.put("methodName", method.getName());
      currentCallInfo.put("timeStr", timeStr);
      String jsonInfo = JsonUtils.asJson(currentCallInfo);
      auditLog.info(jsonInfo);
    }
  }
}
