package org.apache.hadoop.security.tauth;

import com.google.common.base.Preconditions;
import com.tencent.tdw.security.authentication.tauth.AuthServer;
import com.tencent.tdw.security.authentication.tauth.ServerAuthResult;
import com.tencent.tdw.security.netbeans.AuthTicket;
import com.tencent.tdw.security.netbeans.ServiceTicket;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.AuthConfigureHolder;
import org.apache.hadoop.security.AuthConfigureHolder.ProtocolPolicyManagement;

import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import java.nio.ByteBuffer;
import java.util.Map;

public final class TAuthSaslServer extends TAuthSaslBase implements SaslServer {

  private static final Log LOG = LogFactory.getLog(TAuthSaslServer.class);
  private AuthServer authServer;
  private boolean authEnable;
  private ProtocolPolicyManagement protocolPolicyManagement = AuthConfigureHolder.getProtocolPolicyManagement();

  private String authId;

  public TAuthSaslServer(String protocol, String serverName, Map<String, ?> props, CallbackHandler cbh) {
    super(protocol, serverName, props, cbh);
    this.authEnable = AuthConfigureHolder.isAuthEnable();
  }

  @Override
  public byte[] evaluateResponse(byte[] response) throws SaslException {

    throwIfComplete();

    if (response == null) {
      throw new IllegalArgumentException("Received null response");
    }
    String principle = null;
    try {
      if (authServer == null) {
        authServer = AuthServer.getDefault();
      }
      ByteBuffer byteBuffer = ByteBuffer.wrap(response);
      HeaderState state = HeaderState.getByOrdinal(byteBuffer.get());
      if (state == HeaderState.NEGOTIATE) {
        byte[] protocolBytes = new byte[byteBuffer.capacity() - 1];
        byteBuffer.get(protocolBytes);
        String remoteProtocolName = new String(protocolBytes, UTF_8);
        if (tauthProtocolName != null) {
          Preconditions.checkState(tauthProtocolName.equals(remoteProtocolName));
        }
        boolean needAuth = authEnable && protocolPolicyManagement.isNeedAuth(remoteProtocolName);
        ByteBuffer retBuffer = ByteBuffer.allocate(2);
        retBuffer.put((byte) HeaderState.NEGOTIATE_RESPONSE.ordinal());
        retBuffer.put(needAuth
            ? (byte) AuthServiceState.ENABLE.ordinal()
            : (byte) AuthServiceState.DISABLE.ordinal());
        if (LOG.isDebugEnabled()) {
          LOG.debug(remoteProtocolName + " need auth :" + needAuth);
        }
        return retBuffer.array();
      } else {
        Preconditions.checkArgument(state == HeaderState.USE_TAUTH || state == HeaderState.USE_PLAIN_AUTH);
        int protocolLen = byteBuffer.getInt();
        byte[] protocolBytes = new byte[protocolLen];
        byteBuffer.get(protocolBytes);
        String remoteProtocolName = new String(protocolBytes, UTF_8);
        if (tauthProtocolName != null) {
          Preconditions.checkState(tauthProtocolName.equals(remoteProtocolName));
        }
        boolean needAuth = authEnable && protocolPolicyManagement.isNeedAuth(remoteProtocolName);
        if (needAuth) {
          int encryptedAuthenticatorLen = byteBuffer.getInt();
          byte[] encryptedAuthenticator = new byte[encryptedAuthenticatorLen];
          byteBuffer.get(encryptedAuthenticator);
          int serviceTicketLen = byteBuffer.getInt();
          byte[] serviceTicket = new byte[serviceTicketLen];
          byteBuffer.get(serviceTicket);

          final AuthTicket authTicket = new AuthTicket(serviceTicket, encryptedAuthenticator);
          ServerAuthResult serverAuthResult = authServer.auth(authTicket);
          Preconditions.checkNotNull(serverAuthResult);
          ServiceTicket st = serverAuthResult.getTicket();
          sessionKey = st.getSessionKey();
          setNegotiatedQop(getHighestQop());

          principle = st.getAuthenticator().getPrinciple();
          if (tauthProtocolName != null) {
            Preconditions.checkState(tauthProtocolName.equals(st.getProtocol()));
          }
          this.authId = principle.toLowerCase();
          setCompleted(true);
        } else {
          setNegotiatedQop(QOP.AUTH);
          byte[] userBytes = new byte[byteBuffer.getInt()];
          byteBuffer.get(userBytes);
          principle = new String(userBytes);
          this.authId = principle.toLowerCase();
          if (AuthConfigureHolder.isNotAllow(principle)) {
            throw new SaslException("Not allow user :" + principle);
          }
          setCompleted(true);
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("User[" + principle + "] authentication is successful:" + (needAuth ? "by tauth" : "plain"));
        }
        return makeSuccessfulResponse();
      }
    } catch (Exception e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("User[" + principle + "] auth failed");
      }
      throw new SaslException("TAUTH auth failed: " + e.getMessage(), e);
    }
  }

  private byte[] makeSuccessfulResponse() {
    byte[] response = new byte[1 + 1 + 4 + 4];
    ByteBuffer byteBuffer = ByteBuffer.wrap(response);
    byteBuffer.put((byte) HeaderState.AUTH_SUCCESS.ordinal());
    byteBuffer.put((byte) negotiatedQop.getLevel());
    return byteBuffer.array();
  }

  @Override
  public String getAuthorizationID() {
    return authId;
  }

  @Override
  public void dispose() throws SaslException {
    super.dispose();
    authId = null;
  }
}
