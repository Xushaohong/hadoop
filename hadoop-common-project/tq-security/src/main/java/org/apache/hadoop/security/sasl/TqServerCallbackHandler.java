package org.apache.hadoop.security.sasl;


import com.tencent.tdw.security.Tuple;
import org.apache.hadoop.security.sasl.callback.AuthCheckerCallback;
import org.apache.hadoop.security.sasl.callback.AuthenticationCallback;
import org.apache.hadoop.security.sasl.callback.SuccessResponseCallback;
import org.apache.hadoop.security.sasl.callback.ServiceNameCallback;
import org.apache.hadoop.security.sasl.callback.UserCheckerCallback;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import java.io.IOException;

public abstract class TqServerCallbackHandler implements CallbackHandler {

  @Override
  public void handle(Callback[] callbacks) throws IOException {
    if (callbacks == null || callbacks.length == 0) {
      return;
    }

    for (Callback callback : callbacks) {
      if (callback instanceof ServiceNameCallback) {
        handle((ServiceNameCallback) callback);
      } else if (callback instanceof AuthCheckerCallback) {
        handle((AuthCheckerCallback) callback);
      } else if (callback instanceof AuthenticationCallback) {
        handle((AuthenticationCallback) callback);
      } else if (callback instanceof SuccessResponseCallback) {
        handle((SuccessResponseCallback) callback);
      } else if (callback instanceof UserCheckerCallback) {
        handle((UserCheckerCallback) callback);
      }
    }
  }

  public void handle(ServiceNameCallback callback) {
    callback.setServiceName(getServiceName());
  }

  public void handle(AuthCheckerCallback callback) {
    callback.setNeedAuth(isNeedAuth(callback.getExtraId()));
  }

  public void handle(AuthenticationCallback callback) {
    try {
      Tuple<byte[], String> ret = processResponse(callback.getResponse());
      callback.set(ret._1(), ret._2());
    } catch (Exception e) {
      callback.setException(e);
    }
  }

  public void handle(SuccessResponseCallback callback) {
    callback.setResponse(getSuccessResponse(callback.getAuthorizationID()));
  }

  public void handle(UserCheckerCallback callback) {
    callback.setForbidden(isForbidden(callback.getUserName()));
  }

  protected boolean isForbidden(String userName) {
    return false;
  }

  protected byte[] getSuccessResponse(String authorizationID) {
    return null;
  }

  protected boolean isNeedAuth(byte[] extraId) {
    return true;
  }

  protected String getServiceName() {
    return TqSaslServer.DEFAULT_SERVER_NAME;
  }

  protected abstract Tuple<byte[], String> processResponse(byte[] response) throws Exception;

}