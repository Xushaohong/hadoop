package org.apache.hadoop.security.sasl.callback;

import javax.security.auth.callback.Callback;

public class AuthCheckerCallback implements Callback {
  private byte[] extraId;
  private boolean needAuth = true;

  public AuthCheckerCallback() {

  }

  public AuthCheckerCallback(byte[] extraId) {
    this.extraId = extraId;
  }

  public byte[] getExtraId() {
    return extraId;
  }

  public void setNeedAuth(boolean needAuth) {
    this.needAuth = needAuth;
  }

  public boolean isNeedAuth() {
    return needAuth;
  }
}

