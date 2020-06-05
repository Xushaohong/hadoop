package org.apache.hadoop.security.sasl;

import java.nio.ByteBuffer;

public abstract class TqSaslToken {
  abstract byte[] toBytes();

  public static void writeBytes(byte[] bytes, ByteBuffer buffer) {
    TqSaslMessage.writeBytes(bytes, buffer);
  }

  public static byte[] readBytes(ByteBuffer buffer) {
    return TqSaslMessage.readBytes(buffer);
  }
}
