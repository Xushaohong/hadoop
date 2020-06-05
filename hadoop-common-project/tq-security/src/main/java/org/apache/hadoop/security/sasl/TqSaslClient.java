package org.apache.hadoop.security.sasl;


import com.google.common.base.Preconditions;
import com.tencent.tdw.security.Tuple;
import com.tencent.tdw.security.utils.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.sasl.callback.ForwardResponseCallback;
import org.apache.hadoop.security.sasl.callback.ProcessChallengeCallback;
import org.apache.hadoop.security.sasl.callback.UserClientCallback;

import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.SaslException;
import java.util.Map;


public class TqSaslClient extends TqSaslBase implements javax.security.sasl.SaslClient {
  public static final Log LOG = LogFactory.getLog(TqSaslClient.class);

  private String authId;
  private String authUser;
  private byte[] extraId;
  private TqClientCallbackHandler ccbh;

  public TqSaslClient(String authId, String protocol, String serverName, Map<String, ?> props, CallbackHandler cbh) {
    super(protocol, serverName, props, cbh);
    this.authId = authId;
    if (LOG.isDebugEnabled()) {
      StringBuilder builder = new StringBuilder();
      builder.append("SaslClient Init{")
          .append("authId:").append(authId)
          .append(", protocol:").append(protocol)
          .append(", serverName:").append(serverName)
          .append(", props:").append(props)
          .append(", cbh").append(cbh)
          .append("}");
      LOG.debug(builder);
    }
    Preconditions.checkArgument(cbh instanceof TqClientCallbackHandler,
        "CallbackHandler is not instance of  " + TqClientCallbackHandler.class.getName());
    this.ccbh = (TqClientCallbackHandler) cbh;
  }

  @Override
  public boolean hasInitialResponse() {
    return true;
  }


  @Override
  public byte[] evaluateChallenge(byte[] challenge) throws SaslException {
    throwIfComplete();
    try {
      // Client first, send init message
      if (challenge == null || challenge.length == 0) {
        TqSaslToken initToken = buildInitToken();
        return toBytes(new TqSaslMessage(TqSaslState.INITIATE, initToken));
      }

      // Process server's message
      TqSaslMessage saslMessage = TqSaslMessage.fromBytes(challenge);
      TqSaslMessage replay = null;
      if (LOG.isDebugEnabled()) {
        LOG.debug("Received :" + saslMessage.getState());
      }
      switch (saslMessage.getState()) {
        case NEGOTIATE: {
          TqNegotiationToken negotiationToken = TqNegotiationToken.valueOf(saslMessage.getToken());
          if (LOG.isDebugEnabled()) {
            LOG.debug("Received negotiation from " + negotiationToken.getService()
                + ", and ask auth:" + negotiationToken.getService());
          }
          if (negotiationToken.isNeedAuth()) {
            String remoteServiceName = negotiationToken.getService();
            replay = new TqSaslMessage(TqSaslState.RESPONSE, askResponse(
               !StringUtils.isBlank(remoteServiceName) && !remoteServiceName.equals(TqSaslServer.DEFAULT_SERVER_NAME)
                    ? remoteServiceName
                    : serverName));
          } else {
            TqSaslToken initToken = buildInitToken();
            replay = new TqSaslMessage(TqSaslState.INITIATE, initToken);
          }
          break;
        }
        case CHALLENGE: {
          TqChallengeToken challengeToken = TqChallengeToken.valueOf(saslMessage.getToken());
          if (LOG.isDebugEnabled()) {
            LOG.debug("Received challenge from " + challengeToken.getService());
          }
          replay = new TqSaslMessage(TqSaslState.RESPONSE, askResponse(challengeToken.getService()));
          break;
        }
        case SUCCESS: {
          TqSuccessToken successToken = TqSuccessToken.valueOf(saslMessage.getToken());
          QOP qop = QOP.getByLevel(successToken.getQop());
          setNegotiatedQop(qop);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Received token:" + successToken + ", qop:" + qop);
          }
          byte[] response = successToken.getResponse();
          if (response != null) {
            successToken.validate(sessionKey);
            processSuccessResponse(response);
          }
          setCompleted(true);
          break;
        }
        default:
          throw new SaslException(
              "RPC client doesn't support SASL " + saslMessage.getState());
      }
      if (saslMessage.getState() == TqSaslState.SUCCESS) {
        return null;
      }
      Preconditions.checkNotNull(replay);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Replay " + replay.getState() + " for " + authUser);
      }
      return toBytes(replay);
    } catch (Exception e) {
      if (e instanceof SaslException) {
        throw (SaslException) e;
      }
      throw new SaslException("Auth failed: " + e.getMessage(), e);
    }
  }

  private void processSuccessResponse(byte[] response) throws Exception {
    ForwardResponseCallback callback = new ForwardResponseCallback(response);
    ccbh.handle(callback);
  }

  private byte[] askResponse(String service) throws Exception {
    ProcessChallengeCallback callback = new ProcessChallengeCallback(service);
    ccbh.handle(callback);
    Tuple<byte[], byte[]> ret = callback.get();
    sessionKey = ret._1();
    return ret._2();
  }

  private TqInitToken buildInitToken() throws Exception {
    if (authUser == null) {
      UserClientCallback userClientCallback = new UserClientCallback();
      ccbh.handle(userClientCallback);
      authUser = userClientCallback.getUserName();
      if (authUser == null) {
        authUser = authId;
      }
      extraId = userClientCallback.getExtraId();
    }
    Preconditions.checkArgument(authUser != null, "Not found auth user");
    if (LOG.isDebugEnabled()) {
      LOG.debug(authUser + " preparing to auth");
    }
    return new TqInitToken(authUser, extraId);
  }
}
