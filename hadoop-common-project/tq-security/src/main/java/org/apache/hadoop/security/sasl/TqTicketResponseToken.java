package org.apache.hadoop.security.sasl;


import com.google.common.base.Preconditions;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class TqTicketResponseToken extends TqSaslToken {
  private byte[] authenticator;
  private byte[] serviceTicket;
  private int smkId = 0;

  public TqTicketResponseToken(byte[] authenticator, byte[] serviceTicket, int smkId) {
    this.authenticator = authenticator;
    this.serviceTicket = serviceTicket;
    this.smkId = smkId;
  }

  public byte[] getAuthenticator() {
    return authenticator;
  }

  public byte[] getServiceTicket() {
    return serviceTicket;
  }

  public int getSmkId() {
    return smkId;
  }

  @Override
  public byte[] toBytes() {
    int length = 4 + authenticator.length + 4 + serviceTicket.length + 4;
    ByteBuffer buffer = ByteBuffer.allocate(length);
    writeBytes(authenticator,buffer);
    writeBytes(serviceTicket,buffer);
    buffer.putInt(smkId);
    return buffer.array();
  }

  public static TqTicketResponseToken valueOf(byte[] bytes) {
    Preconditions.checkArgument(bytes != null && bytes.length > 0);
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    byte[] authenticator = readBytes(buffer);
    byte[] serviceTicket = readBytes(buffer);
    int smkId = buffer.getInt();
    return new TqTicketResponseToken(authenticator, serviceTicket, smkId);
  }

  @Override
  public String toString() {
    return "ResponseToken{" +
        "authenticator=" + Arrays.toString(authenticator) +
        ", serviceTicket=" + Arrays.toString(serviceTicket) +
        '}';
  }
}
