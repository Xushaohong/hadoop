package org.apache.hadoop.security.token;

import com.google.common.annotations.VisibleForTesting;
import com.tencent.tdw.security.authentication.AuthType;
import com.tencent.tdw.security.authentication.Authentication;
import com.tencent.tdw.security.authentication.LocalKeyManager;
import com.tencent.tdw.security.authentication.SecretKeyProvider;
import com.tencent.tdw.security.authentication.TokenAuthentication;
import com.tencent.tdw.security.authentication.client.CancelTokenRequest;
import com.tencent.tdw.security.authentication.client.RenewTokenRequest;
import com.tencent.tdw.security.authentication.client.SecureClientFactory;
import com.tencent.tdw.security.authentication.client.TokenProtocol;
import com.tencent.tdw.security.utils.Action;
import com.tencent.tdw.security.utils.EncryptUtils;
import com.tencent.tdw.security.utils.Try;
import org.apache.commons.collections.map.LRUMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TqTokenIdentifier extends AbstractDelegationTokenIdentifier {
  private static final Logger LOG = LoggerFactory.getLogger(
      TqTokenIdentifier.class);
  private static final int VERSION = 0;
  public static final Text TQ_TOKEN = new Text("TQ_TOKEN");
  public static final Text NORMAL_KIND = new Text("TQ_NORMAL_TOKEN");
  private Text realKind;
  private String rawToken;
  private String authentication;
  private byte[] password;
  private Map<String, String> props = new HashMap<>();

  @SuppressWarnings("unchecked")
  private static Map<TokenIdentifier, UserGroupInformation> ugiCache =
      Collections.synchronizedMap(new LRUMap(64));


  public TqTokenIdentifier() {
  }

  public TqTokenIdentifier(String tqToken, Map<String, String> props) {
    initialize(tqToken, props);
  }

  private void initialize(String tqToken, Map<String, String> props) {
    Authentication authentication = Authentication.valueOf(tqToken);
    AuthType authType = authentication.type();
    switch (authType) {
      case TOKEN:
        TokenAuthentication tokenAuthentication = (TokenAuthentication) authentication;
        com.tencent.tdw.security.authentication.Token authToken =
            com.tencent.tdw.security.authentication.Token.valueOf(tokenAuthentication.getToken());
        com.tencent.tdw.security.authentication.TokenIdentifier tokenIdentifier = authToken.getTokenIdentifier();
        String tokenSignature = authToken.getSignature();
        setId(tokenIdentifier.getId());
        setOwner(new Text(tokenIdentifier.getOwner()));
        setRenewer(tokenIdentifier.getRenewer() != null
            ? new Text(tokenIdentifier.getRenewer())
            : new Text(tokenIdentifier.getOwner()));
        setRealUser(tokenIdentifier.getRealUser() != null ? new Text(tokenIdentifier.getRealUser()) : new Text());
        setIssueDate(tokenIdentifier.getIssueTime());
        setMaxDate(tokenIdentifier.getMaxTime());
        this.realKind = NORMAL_KIND;
        this.rawToken = tokenAuthentication.getToken();
        this.authentication = tqToken;
        this.password = Try.apply(new Action<byte[]>() {
          @Override
          public byte[] apply() throws Exception {
            return EncryptUtils.hmacSHA1(EncryptUtils.deBase64(tokenSignature), authToken.toToken().getBytes());
          }
        });
        this.props = props;
        return;
      default:
        throw new UnsupportedOperationException("Not support the authentication : " + tqToken);
    }
  }

  @Override
  public UserGroupInformation getUser() {
    UserGroupInformation ugi = ugiCache.get(this);
    if (ugi == null) {
      ugi = super.getUser();
      ugiCache.put(this, ugi);
    }
    return ugi;
  }

  public Map<String, String> getProps() {
    return props;
  }

  @Override
  public Text getKind() {
    return TQ_TOKEN;
  }

  public Text getRealKind() {
    return realKind;
  }

  public String getAuthentication() {
    return authentication;
  }

  public String getRawToken() {
    return rawToken;
  }

  public byte[] getPassword() {
    return password;
  }

  @Override
  public byte[] getBytes() {
    DataOutputBuffer buf = new DataOutputBuffer(4096);
    try {
      this.write(buf);
    } catch (IOException ie) {
      throw new RuntimeException("i/o error in getBytes", ie);
    }
    return Arrays.copyOf(buf.getData(), buf.getLength());
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.write(TQ_TOKEN.getBytes());
    out.writeInt(VERSION);
    int propsSize = props != null ? props.size() : 0;
    out.writeInt(propsSize);
    if (propsSize > 0) {
      for (Map.Entry<String, String> entry : props.entrySet()) {
        new Text(entry.getKey()).write(out);
        new Text(entry.getValue()).write(out);
      }
    }
    new Text(authentication).write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    byte[] headerBytes = new byte[TQ_TOKEN.getBytes().length];
    in.readFully(headerBytes);
    if (!Arrays.equals(headerBytes, TQ_TOKEN.getBytes())) {
      throw new IOException("Not tq token : " + new String(headerBytes));
    }
    int version = in.readInt();
    if (version != VERSION) {
      throw new IOException("Unknown version " + version +
          " in tq token.");
    }
    Map<String, String> tmpProps = new HashMap<>();
    int propsSize = in.readInt();
    for (int i = 0; i < propsSize; i++) {
      Text key = new Text();
      Text value = new Text();
      key.readFields(in);
      value.readFields(in);
      tmpProps.put(key.toString(), value.toString());
    }
    Text tokenIdent = new Text();
    tokenIdent.readFields(in);
    initialize(tokenIdent.toString(), tmpProps);
  }

  @Override
  public String toString() {
    return "TqTokenIdentifier{" +
        "id=" + getId() +
        ", owner=" + getOwner() +
        ", renewer=" + getRenewer() +
        ", realUser=" + getRealUser() +
        ", issueDate=" + getIssueDate() +
        ", maxDate=" + getMaxDate() +
        ", realKind=" + realKind +
        ", rawToken='" + rawToken + '\'' +
        ", tokenIdent=" + authentication +
        ", props=" + props +
        '}';
  }

  public static class TqTokenRenewer extends TokenRenewer {
    @VisibleForTesting

    @Override
    public boolean handleKind(Text realKind) {
      return realKind.equals(TqTokenIdentifier.TQ_TOKEN);
    }

    @Override
    public boolean isManaged(Token<?> token) throws IOException {
      TqTokenIdentifier tqTokenIdentifier = decodeTqTokenIdentifier(token);
      return Boolean.parseBoolean(StringUtils.getPropOrEnvVar("tq.token.managed",
          tqTokenIdentifier.getProps(), Boolean.TRUE.toString()));
    }

    private static TqTokenIdentifier decodeTqTokenIdentifier(Token<?> token) throws IOException {
      TqTokenIdentifier tqTokenIdentifier = (TqTokenIdentifier) token.decodeIdentifier();
      if (tqTokenIdentifier == null) {
        throw new IllegalArgumentException("Can't to decode token : " + token);
      }
      return tqTokenIdentifier;
    }

    @Override
    public long renew(Token<?> token, Configuration conf) throws IOException, InterruptedException {
      TokenProtocol tokenClient = getTokenProtocol(conf);
      String userName = tokenClient.getUser();
      TqTokenIdentifier tqTokenIdentifier = decodeTqTokenIdentifier(token);
      String renewer = findRenewer(tqTokenIdentifier);
      String rawToken = tqTokenIdentifier.getRawToken();
      try {
        if (userName.equals(renewer)) {
          return tokenClient.renewToken(new RenewTokenRequest(rawToken));
        }
        return tokenClient.renewToken(new RenewTokenRequest(rawToken), renewer);
      } catch (Exception e) {
        LOG.warn("Failed to renew token : " + token);
        throw new IOException("Failed to renew token : " + token, e);
      }
    }

    @Override
    public void cancel(Token<?> token, Configuration conf) throws IOException, InterruptedException {
      TokenProtocol tokenClient = getTokenProtocol(conf);
      String userName = tokenClient.getUser();
      TqTokenIdentifier tqTokenIdentifier = decodeTqTokenIdentifier(token);
      String renewer = findRenewer(tqTokenIdentifier);
      String rawToken = tqTokenIdentifier.getRawToken();
      try {
        if (userName.equals(renewer)) {
          tokenClient.cancel(new CancelTokenRequest(rawToken));
          return;
        }
        tokenClient.cancel(new CancelTokenRequest(rawToken), renewer);
      } catch (Exception e) {
        LOG.warn("Failed to cancel token : " + token);
        throw new IOException("Failed to cancel token  : " + token, e);
      }
    }
  }

  private static TokenProtocol getTokenProtocol(Configuration conf) throws IOException {
    TokenProtocol tokenClient = null;
    if (conf.getBoolean("tq.token.manager.custom", false)) {
      String manager = conf.get("tq.token.manager", UserGroupInformation.getCurrentUser().getShortUserName());
      LocalKeyManager keyManager = null;
      String key = conf.get("tq.token.manager.default.key");
      if (key != null && key.length() > 0) {
        keyManager = LocalKeyManager.generateByDefaultKey(key);
      }
      if (keyManager == null) {
        boolean keyLoadLogin = conf.getBoolean("tq.token.manager.load.login.key",
            true);
        String keyDir = conf.get("tq.token.manager.key.dir",
            keyLoadLogin
                ? SecretKeyProvider.getLoginUserKeyPath(true)
                : SecretKeyProvider.getKeyPath(manager, true));
        keyManager = LocalKeyManager.generateByDir(keyDir, false);
      }
      tokenClient = SecureClientFactory.generateTokenClient(manager, keyManager);
    }
    if (tokenClient == null) {
      tokenClient = SecureClientFactory.generateTokenClient();
    }
    return tokenClient;
  }

  private static String findRenewer(TqTokenIdentifier tqTokenIdentifier) {
    String renewer = null;
    if (tqTokenIdentifier.getRenewer() != null) {
      renewer = tqTokenIdentifier.getRenewer().toString();
    }
    if (renewer == null) {
      renewer = tqTokenIdentifier.getOwner().toString();
    }
    return renewer;
  }
}
