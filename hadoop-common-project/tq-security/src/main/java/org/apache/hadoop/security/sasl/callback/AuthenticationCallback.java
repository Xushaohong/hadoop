package org.apache.hadoop.security.sasl.callback;

import com.tencent.tdw.security.Tuple;

import javax.security.auth.callback.Callback;

public class AuthenticationCallback implements Callback {
  private byte[] response;
  private byte[] sessionKey;
  private String authorizationID;
  private Exception exception;

  public AuthenticationCallback(byte[] response) {
    this.response = response;
  }

  public byte[] getResponse() {
    return response;
  }

  public void set(byte[] sessionKey, String authorizationID) {
    this.sessionKey = sessionKey;
    this.authorizationID = authorizationID;
  }

  public void setException(Exception e){
    this.exception = e;
  }

  public Tuple<byte[],String> get() throws Exception{
    if(exception!=null){
      throw exception;
    }
    return Tuple.of(sessionKey, authorizationID);
  }
}
