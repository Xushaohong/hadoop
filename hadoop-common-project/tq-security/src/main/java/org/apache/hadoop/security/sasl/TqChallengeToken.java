package org.apache.hadoop.security.sasl;


import com.google.common.base.Preconditions;

import java.nio.ByteBuffer;

public class TqChallengeToken extends TqSaslToken {
  private String service;

  public TqChallengeToken(String service) {
    this.service = service;
  }

  public String getService() {
    return service;
  }

  @Override
  public byte[] toBytes() {
    int length = 4;
    byte[] serverBytes = null;
    if (service != null) {
      serverBytes = service.getBytes();
      length += serverBytes.length;
    }
    ByteBuffer buffer = ByteBuffer.allocate(length);
    writeBytes(serverBytes, buffer);
    return buffer.array();
  }

  public static TqChallengeToken valueOf(byte[] bytes) {
    Preconditions.checkArgument(bytes != null && bytes.length > 0);
    byte[] serviceByte = readBytes(ByteBuffer.wrap(bytes));
    if (serviceByte != null) {
      return new TqChallengeToken(new String(serviceByte));
    }
    return new TqChallengeToken(null);
  }

  @Override
  public String toString() {
    return "ChallengeToken{" +
        "service='" + service + '\'' +
        '}';
  }
}
