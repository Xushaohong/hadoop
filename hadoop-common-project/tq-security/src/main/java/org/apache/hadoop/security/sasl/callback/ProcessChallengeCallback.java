package org.apache.hadoop.security.sasl.callback;

import com.tencent.tdw.security.Tuple;

import javax.security.auth.callback.Callback;

public class ProcessChallengeCallback implements Callback {
  private String target;
  private byte[] sessionKey;
  private byte[] response;
  private Exception exception;

  public ProcessChallengeCallback(String target) {
    this.target = target;
  }

  public String getTarget() {
    return target;
  }

  public void set(byte[] sessionKey, byte[] response) {
    this.sessionKey = sessionKey;
    this.response = response;
  }

  public void set(Exception e) {
    this.exception = e;
  }

  public Tuple<byte[], byte[]> get() throws Exception {
    if (exception != null) {
      throw exception;
    }
    return Tuple.of(sessionKey, response);
  }

  public byte[] getSessionKey() {
    return sessionKey;
  }

  public byte[] getResponse() {
    return response;
  }

}

