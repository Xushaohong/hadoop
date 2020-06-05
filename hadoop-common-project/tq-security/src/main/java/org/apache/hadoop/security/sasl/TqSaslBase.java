package org.apache.hadoop.security.sasl;

import com.tencent.tdw.security.exceptions.SecureException;
import com.tencent.tdw.security.utils.EncryptUtils;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class TqSaslBase {
  private static final Log LOG = LogFactory.getLog(TqSaslBase.class);
  private static final int DEFAULT_MAX_BUF = 65536;
  protected static final Charset UTF_8 = StandardCharsets.UTF_8;
  protected boolean privacy = false;
  protected boolean integrity = false;
  protected boolean completed = false;
  protected Set<QOP> qops;
  protected QOP negotiatedQop = QOP.AUTH;
  protected byte[] sessionKey;
  protected String protocol;
  protected String serverName;
  protected int maxBufSize = DEFAULT_MAX_BUF;
  protected int rawSendSize = DEFAULT_MAX_BUF - 8;

  protected Map<String, ?> props;
  protected CallbackHandler cbh;

  public TqSaslBase(String protocol, String serverName, Map<String, ?> props, CallbackHandler cbh) {
    this.protocol = protocol;
    this.serverName = serverName;
    this.props = props;
    this.cbh = cbh;
    this.qops = new HashSet<>(Arrays.asList(QOP.parseQop((String) props.get(Sasl.QOP))));
  }

  protected void setNegotiatedQop(QOP qop) {
    this.negotiatedQop = qop;
    if (negotiatedQop == QOP.AUTH_CONF) {
      this.privacy = true;
      this.integrity = true;
    } else if (negotiatedQop == QOP.AUTH_INIT) {
      this.integrity = true;
    }
  }

  public String getMechanismName() {
    return TqAuthConst.TAUTH;
  }

  public String getProtocol() {
    return protocol;
  }

  public TqSaslBase setProtocol(String protocol) {
    this.protocol = protocol;
    return this;
  }

  public String getServerName() {
    return serverName;
  }

  public TqSaslBase setServerName(String serverName) {
    this.serverName = serverName;
    return this;
  }

  public Map<String, ?> getProps() {
    return props;
  }

  public TqSaslBase setProps(Map<String, ?> props) {
    this.props = props;
    return this;
  }

  public CallbackHandler getCbh() {
    return cbh;
  }

  public TqSaslBase setCbh(CallbackHandler cbh) {
    this.cbh = cbh;
    return this;
  }


  public QOP getNegotiatedQop() {
    return negotiatedQop;
  }

  public TqSaslBase setCompleted(boolean completed) {
    this.completed = completed;
    return this;
  }

  public boolean isComplete() {
    return completed;
  }

  public byte[] unwrap(byte[] incoming, int offset, int len)
      throws SaslException {
    throwIfNotComplete();
    if (!(privacy || integrity)) {
      throw new SaslException("Not supported qop");
    }
    try {
      if (privacy) {
        byte[] unwrapped = EncryptUtils.des3Decrypt(sessionKey, incoming, offset, len);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Unwrap data as privacy.Raw offset:" + offset + ", length:" + len
              + ", incoming data length:" + incoming.length
              + ", and unwrapped data length:" + unwrapped.length);
        }
        return unwrapped;
      } else {
        ByteBuffer byteBuffer = ByteBuffer.wrap(incoming, offset, len);
        byte[] remoteHmac = new byte[byteBuffer.getInt()];
        byteBuffer.get(remoteHmac);
        byte[] unwrapped = new byte[byteBuffer.getInt()];
        byteBuffer.get(unwrapped);
        byte[] hmac = EncryptUtils.hmacSHA1(sessionKey, unwrapped);

        if (!Arrays.equals(hmac, remoteHmac)) {
          throw new SaslException("Hmac is not matched, local:" + Base64.encodeBase64String(hmac)
              + ", and remote:" + Base64.encodeBase64String(remoteHmac));
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug("Unwrap data as integrity. Raw offset:" + offset + ", length:" + len
              + ", incoming data length:" + incoming.length
              + ", and unwrapped data length:" + unwrapped.length);
        }
        return unwrapped;
      }
    } catch (SecureException e) {
      throw new SaslException("Unwrap data failed." + " Raw offset:" + offset + ", length:" + len
          + ",incoming data length:" + incoming.length, e);
    }

  }

  public byte[] wrap(byte[] incoming, int offset, int len)
      throws SaslException {
    throwIfNotComplete();
    if (!(privacy || integrity)) {
      throw new SaslException("Not supported qop");
    }

    try {
      if (privacy) {
        byte[] wrapped = EncryptUtils.des3Encrypt(sessionKey, incoming, offset, len);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Wrap data as privacy. Raw offset:" + offset + ", length:" + len
              + ", incoming data length:" + incoming.length
              + ", and wrapped data length:" + wrapped.length);
        }
        return wrapped;
      } else {
        byte[] hmac = EncryptUtils.hmacSHA1(sessionKey, incoming, offset, len);
        long wrapLen = hmac.length + len + 8L;
        if (wrapLen > Integer.MAX_VALUE) {
          throw new SaslException("Wrapped length exceed " + Integer.MAX_VALUE);
        }
        byte[] wrapped = new byte[(int) wrapLen];
        ByteBuffer lenBuffer = ByteBuffer.allocate(4);
        lenBuffer.putInt(hmac.length);
        System.arraycopy(lenBuffer.array(), 0, wrapped, 0, 4);
        System.arraycopy(hmac, 0, wrapped, 4, hmac.length);

        lenBuffer.clear();
        lenBuffer.putInt(len);
        System.arraycopy(lenBuffer.array(), 0, wrapped, 4 + hmac.length, 4);
        System.arraycopy(incoming, offset, wrapped, 8 + hmac.length, len);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Wrap data as integrity. Raw offset:" + offset + ", length:" + len
              + ", incoming data length:" + incoming.length
              + ", and wrapped data length:" + wrapped.length);
        }
        return wrapped;
      }
    } catch (SecureException e) {
      throw new SaslException("Wrap data failed." + "Raw offset:" + offset + ",length:" + len
          + ",incoming data length:" + incoming.length, e);
    }
  }

  protected void throwIfNotComplete() {
    if (!completed) {
      throw new IllegalStateException("Authentication not completed");
    }
  }

  protected void throwIfComplete() {
    if (completed) {
      throw new IllegalStateException("Authentication is completed");
    }
  }

  public String getNegotiatedProperty(String paramString) {
    throwIfNotComplete();

    switch (paramString) {
      case Sasl.QOP:
        if (this.privacy) {
          return "auth-conf";
        }
        if (this.integrity) {
          return "auth-int";
        }
        return "auth";
      case Sasl.MAX_BUFFER:
        return String.valueOf(maxBufSize);
      case Sasl.RAW_SEND_SIZE:
        return String.valueOf(rawSendSize);
    }
    return String.valueOf(props.get(paramString));
  }

  public void dispose() throws SaslException {
    this.privacy = false;
    this.integrity = false;
  }
  static byte[] toBytes(TqSaslMessage saslMessage) throws IOException {
    if (saslMessage == null) {
      throw new IllegalArgumentException("Sasl Message is null");
    }
    return saslMessage.toBytes();
  }

  static String toLowerCase(String str) {
    return str.toLowerCase();
  }

}
