package org.apache.hadoop.security.sasl;

import com.google.common.base.Preconditions;
import com.tencent.tdw.security.exceptions.HmacException;
import com.tencent.tdw.security.utils.EncryptUtils;

import javax.security.sasl.SaslException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class TqSuccessToken extends TqSaslToken {
  private int qop;
  private byte[] response;
  private byte[] digest;

  public TqSuccessToken(int qop, byte[] response, byte[] digest) {
    this.qop = qop;
    this.response = response;
    this.digest = digest;
  }

  public int getQop() {
    return qop;
  }

  public byte[] getResponse() {
    return response;
  }

  public byte[] getDigest() {
    return digest;
  }

  @Override
  public byte[] toBytes() {
    int length = 1 + 4 + 4;
    if (digest != null) {
      length += digest.length;
    }
    if (response != null) {
      length += response.length;
    }
    ByteBuffer buffer = ByteBuffer.allocate(length);
    buffer.put((byte) qop);
    writeBytes(response, buffer);
    writeBytes(digest, buffer);
    return buffer.array();
  }

  public static TqSuccessToken valueOf(byte[] bytes) {
    Preconditions.checkArgument(bytes != null && bytes.length > 0);
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    int qop = buffer.get();
    byte[] response = readBytes(buffer);
    byte[] digest = readBytes(buffer);
    return new TqSuccessToken(qop, response, digest);
  }

  public void validate(byte[] key) throws SaslException {
    if (response != null && digest != null) {
      byte [] calcDigest = null;
      try {
        calcDigest = EncryptUtils.hmacSHA1(key, response);
         Arrays.equals(digest, calcDigest);
      }catch(Exception e){
        throw new SaslException(String.format("Digest validation failed, %s - %s",
            Arrays.toString(digest),  Arrays.toString(calcDigest)));
      }
    }
  }

  public static byte[] getDigest(byte[] key, byte[] data) throws HmacException {
    if (key != null && data != null) {
      return EncryptUtils.hmacSHA1(key, data);
    }
    return null;
  }

  @Override
  public String toString() {
    return "SuccessToken{" +
        "qop=" + qop +
        '}';
  }
}
