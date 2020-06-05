package org.apache.hadoop.security.sasl;


import com.google.common.base.Preconditions;

import java.nio.ByteBuffer;

public class TqInitToken extends TqSaslToken {
  private String userName;
  // custom define
  private byte[] extraId;

  public TqInitToken(String userName) {
    this.userName = userName;
  }

  public TqInitToken(String userName, byte[] extraId) {
    this.userName = userName;
    this.extraId = extraId;
  }

  public String getUserName() {
    return userName;
  }

  public byte[] getExtraId() {
    return extraId;
  }

  @Override
  public byte[] toBytes() {
    Preconditions.checkArgument(userName != null, "Real user is null");
    int length = 0;
    byte[] realUserBytes = userName.getBytes();
    length += (4 + realUserBytes.length);
    length += (4 + (extraId != null ? extraId.length : 0));
    ByteBuffer buffer = ByteBuffer.allocate(length);
    writeBytes(realUserBytes, buffer);
    writeBytes(extraId, buffer);
    return buffer.array();
  }

  public static TqInitToken valueOf(byte[] bytes) {
    String realUser = null;
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    byte[] realUserBytes = readBytes(buffer);
    if (realUserBytes != null) {
      realUser = new String(realUserBytes);
    }
    byte[] token = readBytes(buffer);
    return new TqInitToken(realUser, token);
  }

  @Override
  public String toString() {
    return "InitToken{" +
        ", userName='" + userName + '\'' +
        '}';
  }
}
