package org.apache.hadoop.security.tauth;

import com.tencent.tdw.security.exceptions.SecureException;
import com.tencent.tdw.security.utils.EncryptUtils;
import com.tencent.tdw.security.utils.StringUtils;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.Utils;

import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.security.tauth.TAuthConst.TAUTH_PROTOCOL_NAME_KEY;

public class TAuthSaslBase {

  static final int DEFAULT_MAXBUF = 65536;
   boolean privacy = false;
   boolean integrity = false;
   boolean completed = false;
   String tauthProtocolName;
   Set<QOP> qops;
   QOP negotiatedQop = QOP.AUTH;
   byte[] sessionKey;
   String protocol;
   String serverName;
   int maxBufSize = DEFAULT_MAXBUF;
   int rawSendSize = DEFAULT_MAXBUF - 8;

   Map<String, ?> props;
   CallbackHandler cbh;
  static final Charset UTF_8 = StandardCharsets.UTF_8;
  private static final Log LOG = LogFactory.getLog(TAuthSaslBase.class);

  public TAuthSaslBase(String protocol, String serverName, Map<String, ?> props,
      CallbackHandler cbh) {
    this.protocol = protocol;
    this.serverName = serverName;
    this.props = props;
    this.cbh = cbh;
    this.qops = new HashSet<>(Arrays.asList(parseQop((String) props.get(Sasl.QOP))));
    this.tauthProtocolName = (String) props.get(TAUTH_PROTOCOL_NAME_KEY);
  }

  private QOP[] parseQop(String qopProp) {
    if (Utils.isNullOrEmpty(qopProp)) {
      return new QOP[]{QOP.AUTH};
    }
    String[] qopItems = StringUtils.split(qopProp);
    QOP[] qops = new QOP[qopItems.length];
    for (int i = 0; i < qopItems.length; i++) {
      qops[i] = QOP.getByName(qopItems[i]);
      if (qops[i] == null) {
        qops[i] = QOP.AUTH;
      }
    }
    return qops;
  }

  public QOP getHighestQop() {
    int level = QOP.AUTH.level;
    for (QOP qop : qops) {
      if (qop.level > level) {
        level = qop.level;
      }
    }
    return QOP.getByLevel(level);
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
    return TAuthConst.TAUTH;
  }

  public String getProtocol() {
    return protocol;
  }

  public TAuthSaslBase setProtocol(String protocol) {
    this.protocol = protocol;
    return this;
  }

  public String getServerName() {
    return serverName;
  }

  public TAuthSaslBase setServerName(String serverName) {
    this.serverName = serverName;
    return this;
  }

  public Map<String, ?> getProps() {
    return props;
  }

  public TAuthSaslBase setProps(Map<String, ?> props) {
    this.props = props;
    return this;
  }

  public CallbackHandler getCbh() {
    return cbh;
  }

  public TAuthSaslBase setCbh(CallbackHandler cbh) {
    this.cbh = cbh;
    return this;
  }


  public TAuthSaslBase setCompleted(boolean completed) {
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
      throw new IllegalStateException("TAUTH authentication not completed");
    }
  }

  protected void throwIfComplete() {
    if (completed) {
      throw new IllegalStateException("TAUTH authentication is completed");
    }
  }

  public String getNegotiatedProperty(String paramString) {
    if (!this.completed) {
      throw new IllegalStateException("SASL authentication not completed");
    }

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

  public enum HeaderState {
    NEGOTIATE, NEGOTIATE_RESPONSE, USE_TAUTH, USE_PLAIN_AUTH, AUTH_SUCCESS, AUTH_FAILED;

    public static HeaderState getByOrdinal(int ordinal) {
      for (HeaderState state : values()) {
        if (state.ordinal() == ordinal) {
          return state;
        }
      }
      return null;
    }
  }


  public enum AuthServiceState {
    ENABLE, DISABLE;

    public static AuthServiceState getByOrdinal(int ordinal) {
      for (AuthServiceState state : values()) {
        if (state.ordinal() == ordinal) {
          return state;
        }
      }
      return null;
    }
  }

  public enum QOP {
    AUTH_CONF("auth-conf", "privacy", 4), AUTH_INIT("auth-int", "integrity", 2), AUTH("auth",
        "auth", 1);
    private String name;
    private String desc;
    private int level;

    QOP(String name, String desc, int level) {
      this.name = name;
      this.desc = desc;
      this.level = level;
    }


    public String getName() {
      return name;
    }

    public String getDesc() {
      return desc;
    }

    public int getLevel() {
      return level;
    }

    public static QOP getByName(String name) {
      for (QOP qop : values()) {
        if (qop.name.equalsIgnoreCase(name)) {
          return qop;
        }
      }
      return null;
    }

    public static QOP getByLevel(int level) {
      for (QOP qop : values()) {
        if (qop.level == level) {
          return qop;
        }
      }
      return null;
    }
  }
}
