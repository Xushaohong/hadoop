package org.apache.hadoop.security.sasl;

import com.tencent.tdw.security.Tuple;
import org.apache.hadoop.security.sasl.callback.ProcessChallengeCallback;
import org.apache.hadoop.security.sasl.callback.ForwardResponseCallback;
import org.apache.hadoop.security.sasl.callback.UserClientCallback;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import java.io.IOException;

public abstract class TqClientCallbackHandler implements CallbackHandler {

  @Override
  public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
    if (callbacks == null || callbacks.length == 0) {
      return;
    }
    for (Callback callback : callbacks) {
      if (callback instanceof UserClientCallback) {
        handle((UserClientCallback) callback);
      } else if (callback instanceof ProcessChallengeCallback) {
        handle((ProcessChallengeCallback) callback);
      } else if (callback instanceof ForwardResponseCallback) {
        handle((ForwardResponseCallback) callback);
      }
    }
  }

  public void handle(UserClientCallback callback) {
    callback.setUserName(getUserName());
    callback.setExtraId(getExtraId());
  }

  public void handle(ProcessChallengeCallback callback) {
    try {
      Tuple<byte[], byte[]> ret = processChallenge(callback.getTarget());
      callback.set(ret._1(), ret._2());
    } catch (Exception e) {
      callback.set(e);
    }
  }

  public void handle(ForwardResponseCallback callback) {
    handleResponse(callback.getResponse());
  }


  protected void handleResponse(byte[] response){

  }

  protected String getUserName(){
    return null;
  }

  protected byte[] getExtraId(){
    return null;
  }

  protected abstract Tuple<byte[], byte[]> processChallenge(String target) throws Exception;
}