package org.apache.hadoop.security.sasl.callback;

import javax.security.auth.callback.Callback;

public class SuccessResponseCallback implements Callback {
  private byte[] response;
  private String authorizationID;

  public SuccessResponseCallback(String authorizationID) {
    this.authorizationID = authorizationID;
  }

  public String getAuthorizationID() {
    return authorizationID;
  }

  public void setResponse(byte[] response) {
    this.response = response;
  }

  public byte[] getResponse() {
    return response;
  }
}
