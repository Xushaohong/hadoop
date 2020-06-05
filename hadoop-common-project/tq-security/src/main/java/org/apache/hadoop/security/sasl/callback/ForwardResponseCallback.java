package org.apache.hadoop.security.sasl.callback;

import javax.security.auth.callback.Callback;

public class ForwardResponseCallback implements Callback {
  private byte[] response;

  public ForwardResponseCallback(byte[] response) {
    this.response = response;
  }

  public byte[] getResponse() {
    return response;
  }
}
