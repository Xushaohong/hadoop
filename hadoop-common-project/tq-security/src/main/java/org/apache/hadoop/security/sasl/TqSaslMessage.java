package org.apache.hadoop.security.sasl;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;

public class TqSaslMessage {
  private final static int VERSION = 0;
  private TqSaslState state;
  private byte[] token;


  public TqSaslMessage(TqSaslState state, TqSaslToken token) {
    this(state, token.toBytes());
  }

  public TqSaslMessage(TqSaslState state, byte[] token) {
    this.state = state;
    this.token = token;
  }

  public TqSaslState getState() {
    return this.state;
  }

  public byte[] getToken() {
    return this.token;
  }

  public byte[] toBytes() throws IOException {
    int msgLen = token != null ? token.length : 0;
    ByteBuffer buffer = ByteBuffer.allocate(msgLen + 1 + 1 + 4);
    buffer.put((byte) VERSION);
    buffer.put((byte) state.getIndex());
    writeBytes(token, buffer);
    return buffer.array();
  }

  public static TqSaslMessage fromBytes(byte[] bytes) throws IOException {
    Preconditions.checkNotNull(bytes, "Empty message");
    byte[] message = null;
    TqSaslState state;
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    int version = buffer.get();
    if (VERSION != version) {
      throw new IllegalArgumentException("Unknown version : " + version);
    }
    int stateIdx = buffer.get();
    state = TqSaslState.valueOf(stateIdx);
    if (state == null) {
      throw new IllegalArgumentException("Unknown state : " + stateIdx);
    }
    return new TqSaslMessage(state, readBytes(buffer));
  }

  public static void writeBytes(byte[] bytes, ByteBuffer buffer) {
    if (bytes != null) {
      buffer.putInt(bytes.length);
      buffer.put(bytes);
    } else {
      buffer.putInt(0);
    }
  }

  public static byte[] readBytes(ByteBuffer buffer) {
    int length = buffer.getInt();
    if (length > 0) {
      byte[] bytes = new byte[length];
      buffer.get(bytes);
      return bytes;
    }
    return null;
  }
}
