package org.apache.hadoop.security.tauth;

import com.google.common.base.Preconditions;
import com.tencent.tdw.security.authentication.LocalKeyManager;
import com.tencent.tdw.security.authentication.tauth.AuthClient;
import com.tencent.tdw.security.exceptions.SecureException;
import com.tencent.tdw.security.netbeans.AppServerInfo;
import com.tencent.tdw.security.netbeans.AuthTicket;
import com.tencent.tdw.security.netbeans.Ticket;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.Utils;
import org.apache.hadoop.security.tauth.TAuthLoginModule.TAuthCredential;
import org.apache.hadoop.security.tauth.TAuthLoginModule.TAuthPrincipal;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.util.Map;
import java.util.Objects;

public final class TAuthSaslClient extends TAuthSaslBase implements SaslClient {

  public static final Log LOG = LogFactory.getLog(TAuthSaslClient.class);

  private String realUser;
  private String proxyUser;
  private AuthClient client;
  private AppServerInfo serverInfo;
  private byte[] protocolBytes;


  public TAuthSaslClient(String protocol, String serverName, Map<String, ?> props, CallbackHandler cbh) {
    super(protocol, serverName, props, cbh);
  }

  private void initAuthClient()
      throws SecurityException, IOException, UnsupportedCallbackException {
    UserInfoCallback callback = new UserInfoCallback();
    cbh.handle(new Callback[]{callback});
    this.realUser = callback.getRealUser();
    this.proxyUser = callback.getProxyUser();
    LocalKeyManager localKeyManager = callback.getCredential().getLocalKeyManager();
    this.client = new AuthClient(realUser, localKeyManager);
  }

  private void initAuthServerInfo() throws IOException, UnsupportedCallbackException {
    ServerInfoCallback callback = new ServerInfoCallback();
    cbh.handle(new Callback[]{callback});

    InetSocketAddress serverAddress = callback.getAddress();
    String protocolName = callback.getProtocol();
    Preconditions.checkNotNull(protocolName,  "server protocol");
    Preconditions.checkNotNull(serverAddress, "server address");
    serverInfo = new AppServerInfo(Objects.requireNonNull(serverAddress).getAddress().getHostAddress(),
        serverAddress.getPort(), protocolName);
    protocolBytes = serverInfo.getProtocol().getBytes(UTF_8);
  }

  @Override
  public boolean hasInitialResponse() {
    return true;
  }

  @Override
  public byte[] evaluateChallenge(byte[] challenge) throws SaslException {
    throwIfComplete();
    if (challenge == null) {
      throw new IllegalArgumentException("Received null challenge");
    }

    try {
      if (client == null) {
        initAuthClient();
      }

      if (serverInfo == null) {
        initAuthServerInfo();
      }

      //negotiate
      if (challenge.length == 0) {
        int negotiationLen = 1 + protocolBytes.length;
        ByteBuffer buffer = ByteBuffer.allocate(negotiationLen);
        buffer.put((byte) HeaderState.NEGOTIATE.ordinal());
        buffer.put(protocolBytes);
        return buffer.array();
      } else {
        // 1. SUCCESS: AUTH_SUCCESS,QOP_LEVEL,USER_LEN,USER,USER_ADDRESS_LEN,USER_ADDRESS
        // 2. FAILED: AUTH_FAILED,MSG
        ByteBuffer buffer = ByteBuffer.wrap(challenge);
        HeaderState headerState = HeaderState.getByOrdinal(buffer.get());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Received  negotiate state: " + headerState + " for " + realUser);
        }
        if (HeaderState.NEGOTIATE_RESPONSE == headerState) {
          AuthServiceState authServiceState = AuthServiceState.getByOrdinal(buffer.get());
          if (LOG.isDebugEnabled()) {
            LOG.debug("Received auth state: " + authServiceState + " for " + realUser);
          }
          if (AuthServiceState.ENABLE == authServiceState) {
            return negotiateUnderAuth();
          } else {
            return negotiateWithoutAuth();
          }
        } else {
          Preconditions.checkState(HeaderState.AUTH_SUCCESS == headerState
              || HeaderState.AUTH_FAILED == headerState);
          // 1. SUCCESS: AUTH_SUCCESS,QOP_LEVEL,USER_LEN,USER,USER_ADDRESS_LEN,USER_ADDRESS
          // 2. FAILED: AUTH_FAILED,MSG
          if (HeaderState.AUTH_SUCCESS == headerState) {
            setNegotiatedQop(QOP.getByLevel(buffer.get()));
            setCompleted(true);
          } else {
            byte[] msg = new byte[buffer.getInt()];
            buffer.get(msg);
            throw new SaslException(
                "TAUTH auth failed: " + new String(msg, UTF_8) + " for " + realUser);
          }
        }
      }
      return null;
    } catch (Exception e) {
      throw new SaslException("TAUTH auth failed: " + e.getMessage(), e);
    }
  }

  private byte[] negotiateUnderAuth() throws SecureException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Try auth with key for " + realUser);
    }
    Ticket ticket = client.ask(serverInfo, proxyUser);
    Preconditions.checkNotNull(ticket,
        "Client got empty authentication from AuthConfigureHolder for " + realUser);
    AuthTicket authTicket = ticket.getAuthTicket();
    sessionKey = ticket.getSessionTicket().getSessionKey();
    Preconditions.checkNotNull(authTicket, "Client got empty authentication from AuthConfigureHolder");
    Preconditions.checkNotNull(sessionKey, "SessionKey is invalid");
    byte[] encryptedAuthenticator = authTicket.getEncryptedAuthenticator();
    byte[] serviceTicket = authTicket.getServiceTicket();
    int len = 1 + 4 + protocolBytes.length + 4 + encryptedAuthenticator.length + 4 + serviceTicket.length;
    ByteBuffer buffer = ByteBuffer.allocate(len);
    buffer.put((byte) HeaderState.USE_TAUTH.ordinal());
    buffer.putInt(protocolBytes.length);
    buffer.put(protocolBytes);
    buffer.putInt(encryptedAuthenticator.length);
    buffer.put(encryptedAuthenticator);
    buffer.putInt(serviceTicket.length);
    buffer.put(serviceTicket);
    return buffer.array();
  }

  private byte[] negotiateWithoutAuth() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Try auth with plain for " + realUser);
    }
    byte[] userBytes = realUser.getBytes(UTF_8);
    ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + protocolBytes.length + 4 + userBytes.length);
    buffer.put((byte) HeaderState.USE_PLAIN_AUTH.ordinal());
    buffer.putInt(protocolBytes.length);
    buffer.put(protocolBytes);
    buffer.putInt(userBytes.length);
    buffer.put(userBytes);
    return buffer.array();
  }


  public static class ServerInfoCallback implements Callback {

    private String protocol;
    private InetSocketAddress address;

    public String getProtocol() {
      return protocol;
    }

    public void setProtocol(String protocol) {
      this.protocol = protocol;
    }

    public InetSocketAddress getAddress() {
      return address;
    }

    public void setAddress(InetSocketAddress address) {
      this.address = address;
    }
  }

  public static class UserInfoCallback implements Callback {

    private String realUser;
    private String proxyUser;
    private TAuthCredential credential;

    public String getRealUser() {
      return realUser;
    }

    public void setRealUser(String realUser) {
      this.realUser = realUser;
    }

    public String getProxyUser() {
      return proxyUser;
    }

    public void setProxyUser(String proxyUser) {
      this.proxyUser = proxyUser;
    }

    public TAuthCredential getCredential() {
      return credential;
    }

    public void setCredential(TAuthCredential credential) {
      this.credential = credential;
    }
  }


  public static class DefaultTAuthSaslCallbackHandler implements CallbackHandler {

    private String user;
    private String protocol;
    private InetSocketAddress address;


    public DefaultTAuthSaslCallbackHandler(String user, InetSocketAddress address, Class<?> protocolClass) {
      this(user, address, protocolClass.getName());
    }

    public DefaultTAuthSaslCallbackHandler(String user, InetSocketAddress address, String protocol) {
      this.user = user;
      this.address = address;
      this.protocol = protocol.replaceAll("PB", "");
    }


    @Override
    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
      ServerInfoCallback serverInfoCallback = null;
      UserInfoCallback userInfoCallback = null;
      for (Callback callback : callbacks) {
        if (callback instanceof ServerInfoCallback) {
          serverInfoCallback = (ServerInfoCallback) callback;
        }
        if (callback instanceof UserInfoCallback) {
          userInfoCallback = (UserInfoCallback) callback;
        }
      }

      if (serverInfoCallback != null) {
        serverInfoCallback.setProtocol(protocol);
        serverInfoCallback.setAddress(address);
        if (LOG.isDebugEnabled()) {
          LOG.debug("TAuth SASL client callback: get server info, protocol: " + protocol
              + ", address: " + address);
        }
      }

      if (userInfoCallback != null) {
        AccessControlContext context = AccessController.getContext();
        Subject subject = Subject.getSubject(context);
        TAuthCredential credential = TAuthCredential.getFromSubject(subject);
        Preconditions.checkNotNull(credential, TAuthCredential.class.getName());
        TAuthPrincipal principal = TAuthPrincipal.getFromSubject(subject);
        Preconditions.checkNotNull(principal, TAuthPrincipal.class.getName());
        String realUser = principal.getName();

        userInfoCallback.setRealUser(realUser);
        if (Utils.isNotNullOrEmpty(user) && !user.equals(realUser)) {
          userInfoCallback.setProxyUser(user);
        }
        userInfoCallback.setCredential(credential);
        if (LOG.isDebugEnabled()) {
          LOG.debug("TAuth SASL client callback: get user info"
              + ", user: " + realUser);
        }
      }

    }
  }
}
