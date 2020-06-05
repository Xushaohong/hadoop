package org.apache.hadoop.security.sasl.callback;

import javax.security.auth.callback.Callback;

public class UserCheckerCallback implements Callback {
  private String userName;
  private boolean forbidden = false;

  public UserCheckerCallback(String userName) {
    this.userName = userName;
  }

  public String getUserName() {
    return userName;
  }
  
  public void setForbidden(boolean forbidden) {
    this.forbidden = forbidden;
  }

  public boolean isForbidden() {
    return forbidden;
  }
}

