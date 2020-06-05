package org.apache.hadoop.security.sasl.callback;

import javax.security.auth.callback.Callback;

public class UserClientCallback implements Callback {
  private String userName;
  private byte[] extraId;

  public UserClientCallback() {

  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  public String getUserName() {
    return userName;
  }

  public void setExtraId(byte[] extraId) {
    this.extraId = extraId;
  }

  public byte[] getExtraId() {
    return extraId;
  }
}
