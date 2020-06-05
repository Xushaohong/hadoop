package org.apache.hadoop.security.sasl;


import com.google.common.base.Preconditions;
import com.tencent.tdw.security.Tuple;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.sasl.callback.AuthCheckerCallback;
import org.apache.hadoop.security.sasl.callback.AuthenticationCallback;
import org.apache.hadoop.security.sasl.callback.ServiceNameCallback;
import org.apache.hadoop.security.sasl.callback.SuccessResponseCallback;
import org.apache.hadoop.security.sasl.callback.UserCheckerCallback;

import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.SaslException;
import java.util.Arrays;
import java.util.Map;


public class TqSaslServer extends TqSaslBase implements javax.security.sasl.SaslServer {
  public static final Log LOG = LogFactory.getLog(TqSaslServer.class);
  public static final String DEFAULT_SERVER_NAME = "default";
  private String authorizationID;
  private String initAuthorizationID;
  private Boolean needAuth;
  private String authServiceName;
  private TqServerCallbackHandler scbh;

  public TqSaslServer(String protocol, String serverName, Map<String, ?> props, CallbackHandler cbh) {
    super(protocol, serverName, props, cbh);
    if (LOG.isDebugEnabled()) {
      StringBuilder builder = new StringBuilder();
      builder.append("SaslServer Init{")
          .append("protocol:").append(protocol)
          .append(", serverName:").append(serverName)
          .append(", props:").append(props)
          .append("cbh").append(cbh)
          .append("}");
      LOG.debug(builder);
    }
    Preconditions.checkArgument(cbh instanceof TqServerCallbackHandler,
        "CallbackHandler is not instance of  " + TqServerCallbackHandler.class.getName());
    this.scbh = (TqServerCallbackHandler) cbh;
  }

  @Override
  public String getAuthorizationID() {
    return authorizationID;
  }

  @Override
  public byte[] evaluateResponse(byte[] response) throws SaslException {
    throwIfComplete();
    //Server first
    try {
      if (response == null || response.length == 0) {
        return toBytes(new TqSaslMessage(TqSaslState.NEGOTIATE, buildNegotiationToken()));
      }

      // Process client's message
      TqSaslMessage saslMessage = TqSaslMessage.fromBytes(response);
      TqSaslMessage replay;
      if (LOG.isDebugEnabled()) {
        LOG.debug("Received :" + saslMessage.getState());
      }
      switch (saslMessage.getState()) {
        case INITIATE: {
          TqInitToken token = TqInitToken.valueOf(saslMessage.getToken());
          if (LOG.isDebugEnabled()) {
            LOG.debug("Received token:" + token);
          }
          Preconditions.checkArgument(token.getUserName() != null, "Invalid init token");
          String initUser = toLowerCase(token.getUserName());
          this.initAuthorizationID = initUser;
          boolean needAuth = getNeedAuth(token.getExtraId());
          if (LOG.isDebugEnabled()) {
            LOG.debug(initUser + " need auth: " + needAuth);
          }
          if (needAuth) {
            replay = new TqSaslMessage(TqSaslState.CHALLENGE, new TqChallengeToken(getServerName()));
          } else {
            checkAllow(initUser);
            this.authorizationID = initUser;
            replay = new TqSaslMessage(TqSaslState.SUCCESS,
                new TqSuccessToken(QOP.AUTH.getLevel(), getSuccessResp(), null));
            setNegotiatedQop(QOP.AUTH);
          }
          break;
        }
        case RESPONSE: {
          byte[] challengeResp = saslMessage.getToken();
          if (LOG.isDebugEnabled()) {
            LOG.debug("Received response:" + Arrays.asList(challengeResp));
          }
          processResponse(challengeResp);
          byte[] successResp = getSuccessResp();
          if (sessionKey != null) {
            setNegotiatedQop(QOP.getHighestQop(qops));
          } else {
            setNegotiatedQop(QOP.AUTH);
          }
          replay = new TqSaslMessage(TqSaslState.SUCCESS, new TqSuccessToken(getNegotiatedQop().getLevel(),
              successResp, TqSuccessToken.getDigest(sessionKey, successResp)));
          break;
        }
        default:
          throw new SaslException(
              "Server doesn't support SASL " + saslMessage.getState());
      }
      Preconditions.checkNotNull(replay, "Replay is null");
      if (LOG.isDebugEnabled()) {
        LOG.debug("Replay " + replay.getState() + " for " + getAuthServiceName());
      }
      if (replay.getState() == TqSaslState.SUCCESS) {
        setCompleted(true);
        if (LOG.isDebugEnabled()) {
          LOG.debug("User(" + getAuthorizationID() + ") completed sasl");
        }
      }
      return toBytes(replay);
    } catch (Exception e) {
      if (e instanceof SaslException) {
        throw (SaslException) e;
      }
      throw new SaslException("Auth failed: " + e.getMessage(), e);
    }
  }

  private byte[] getSuccessResp() throws Exception {
    SuccessResponseCallback callback = new SuccessResponseCallback(getAuthorizationID());
    scbh.handle(callback);
    return callback.getResponse();
  }

  private TqNegotiationToken buildNegotiationToken() throws Exception {
    String serviceName = getAuthServiceName();
    return new TqNegotiationToken(getNeedAuth(null), serviceName);
  }

  private boolean getNeedAuth(byte[] ident) throws Exception {
    if (needAuth == null) {
      AuthCheckerCallback callback = new AuthCheckerCallback(ident);
      scbh.handle(callback);
      needAuth = callback.isNeedAuth();
    }
    return needAuth;
  }

  private void checkAllow(String userName) throws Exception {
    UserCheckerCallback callback = new UserCheckerCallback(userName);
    scbh.handle(callback);
    if (callback.isForbidden()) {
      throw new SaslException("User(" + userName + ") is forbidden!");
    }
  }

  public String getAuthServiceName() throws Exception {
    if (authServiceName == null) {
      if (DEFAULT_SERVER_NAME.equals(serverName)) {
        ServiceNameCallback callback = new ServiceNameCallback();
        scbh.handle(callback);
        authServiceName = callback.getServiceName();
      }
      if (authServiceName == null) {
        authServiceName = serverName;
      }
    }
    return authServiceName;
  }

  private void processResponse(byte[] response) throws Exception {
    AuthenticationCallback callback = new AuthenticationCallback(response);
    scbh.handle(callback);
    Tuple<byte[], String> ret = callback.get();
    String authId = ret._2();
    Preconditions.checkArgument(authId != null, "Got empty authId!");
    authId = toLowerCase(authId);
    if (this.initAuthorizationID != null) {
      Preconditions.checkArgument(authId.equals(initAuthorizationID),
          String.format("Init authorizationID is not match authId, %s : %s",
              initAuthorizationID, authId));
    }
    this.authorizationID = authId;
    this.sessionKey = ret._1();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Retrieved authorizationID:" + authorizationID + ", sessionKey:" + Arrays.toString(sessionKey));
    }
  }

}
