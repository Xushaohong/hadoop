package org.apache.hadoop.security.sasl;


import com.google.common.base.Preconditions;

import java.nio.ByteBuffer;

public class TqNegotiationToken extends TqSaslToken {
  private boolean needAuth;
  private String service;

  public TqNegotiationToken(boolean needAuth, String service) {
    this.needAuth = needAuth;
    this.service = service;
  }

  public String getService() {
    return service;
  }

  public boolean isNeedAuth() {
    return needAuth;
  }

  @Override
  public byte[] toBytes() {
    int length = 1 + 4;
    byte[] serverBytes = null;
    if (service != null) {
      serverBytes = service.getBytes();
      length += service.getBytes().length;
    }
    ByteBuffer buffer = ByteBuffer.allocate(length);
    buffer.put((byte) (needAuth ? 1 : 0));
    writeBytes(serverBytes, buffer);
    return buffer.array();
  }

  public static TqNegotiationToken valueOf(byte[] bytes) {
    Preconditions.checkArgument(bytes != null && bytes.length > 0);
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    boolean needAuth = buffer.get() == 1;
    byte[] serviceBytes = readBytes(buffer);
    if (serviceBytes != null) {
      return new TqNegotiationToken(needAuth, new String(serviceBytes));
    }
    return new TqNegotiationToken(needAuth, null);
  }

  @Override
  public String toString() {
    return "NegotiationToken{" +
        "needAuth=" + needAuth +
        ", service='" + service + '\'' +
        '}';
  }
}
