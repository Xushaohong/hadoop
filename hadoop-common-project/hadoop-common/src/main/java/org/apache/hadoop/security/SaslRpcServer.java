/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.security;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.security.Security;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import javax.security.sasl.SaslServerFactory;

import com.tencent.tdw.security.Tuple;
import com.tencent.tdw.security.authentication.Authentication;
import com.tencent.tdw.security.authentication.Authenticator;
import com.tencent.tdw.security.authentication.LocalKeyManager;
import com.tencent.tdw.security.authentication.SecurityCenterProvider;
import com.tencent.tdw.security.authentication.TAuthAuthentication;
import com.tencent.tdw.security.authentication.service.SecureService;
import com.tencent.tdw.security.authentication.service.SecureServiceFactory;
import com.tencent.tdw.security.authentication.v2.SecureServiceV2;
import com.tencent.tdw.security.utils.ENVUtils;
import com.tencent.tdw.security.utils.StringUtils;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.Server.Connection;
import org.apache.hadoop.ipc.StandbyException;
import org.apache.hadoop.security.sasl.TqAuthConst;
import org.apache.hadoop.security.sasl.TqServerCallbackHandler;
import org.apache.hadoop.security.sasl.TqServerSecurityProvider;
import org.apache.hadoop.security.sasl.TqTicketResponseToken;
import org.apache.hadoop.security.tauth.TAuthConst;
import org.apache.hadoop.security.tauth.TAuthLoginModule;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TqTokenIdentifier;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility class for dealing with SASL on RPC server
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class SaslRpcServer {
  public static final Logger LOG = LoggerFactory.getLogger(SaslRpcServer.class);
  public static final String SASL_DEFAULT_REALM = "default";
  private static SaslServerFactory saslFactory;

  private static String serviceName;
  private static SecureServiceV2 secureServiceV2;

  public enum QualityOfProtection {
    AUTHENTICATION("auth"),
    INTEGRITY("auth-int"),
    PRIVACY("auth-conf");
    
    public final String saslQop;
    
    private QualityOfProtection(String saslQop) {
      this.saslQop = saslQop;
    }
    
    public String getSaslQop() {
      return saslQop;
    }
  }

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public AuthMethod authMethod;
  public int authVersion;
  public String mechanism;
  public String protocol;
  public String serverId;

  public SaslRpcServer(AuthMethod authMethod) throws IOException {
    this(authMethod, 0);
  }

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public SaslRpcServer(AuthMethod authMethod, int authVersion) throws IOException {
    this.authMethod = authMethod;
    this.authVersion = authVersion;
    mechanism = authMethod.getMechanismName();
    switch (authMethod) {
      case SIMPLE: {
        return; // no sasl for simple
      }
      case TOKEN: {
        protocol = "";
        serverId = SaslRpcServer.SASL_DEFAULT_REALM;
        break;
      }
      case TAUTH:
        protocol = "";
        serverId = SaslRpcServer.SASL_DEFAULT_REALM;

        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        initTauthSecureService(authMethod, ugi);
        if (!StringUtils.isBlank(serviceName)) {
          // replace serverId with serviceName if not null
          serverId = serviceName;
        }
        break;
      case KERBEROS: {
        String fullName = UserGroupInformation.getCurrentUser().getUserName();
        if (LOG.isDebugEnabled())
          LOG.debug("Kerberos principal name is " + fullName);
        // don't use KerberosName because we don't want auth_to_local
        String[] parts = fullName.split("[/@]", 3);
        protocol = parts[0];
        // should verify service host is present here rather than in create()
        // but lazy tests are using a UGI that isn't a SPN...
        serverId = (parts.length < 2) ? "" : parts[1];
        break;
      }
      default:
        // we should never be able to get here
        throw new AccessControlException(
            "Server does not support SASL " + authMethod);
    }
  }
  
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public SaslServer create(final Connection connection,
                           final Map<String,?> saslProperties,
                           SecretManager<TokenIdentifier> secretManager
      ) throws IOException, InterruptedException {
    UserGroupInformation ugi = null;
    final CallbackHandler callback;
    switch (authMethod) {
      case TOKEN: {
        callback = new SaslDigestCallbackHandler(secretManager, connection);
        break;
      }
      case KERBEROS: {
        ugi = UserGroupInformation.getCurrentUser();
        if (serverId.isEmpty()) {
          throw new AccessControlException(
              "Kerberos principal name does NOT have the expected "
                  + "hostname part: " + ugi.getUserName());
        }
        callback = new SaslGssCallbackHandler();
        break;
      }
      case TAUTH:{
        if (authVersion == 0) {
          //no need callback
          callback = null;
        } else if (authVersion == 1) {
          callback = new TqAuthServerCallbackHandler(serverId, secureServiceV2);
        } else {
          throw new IOException("Unknown version of TAUTH " + authVersion);
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("TAUTH mechanism:" + mechanism +", version:" + authVersion);
        }
        break;
      }
      default:
        // we should never be able to get here
        throw new AccessControlException(
            "Server does not support SASL " + authMethod);
    }
    
    final SaslServer saslServer;
    if (ugi != null) {
      saslServer = ugi.doAs(
        new PrivilegedExceptionAction<SaslServer>() {
          @Override
          public SaslServer run() throws SaslException  {
            return saslFactory.createSaslServer(mechanism, protocol, serverId,
                saslProperties, callback);
          }
        });
    } else {
      saslServer = saslFactory.createSaslServer(mechanism, protocol, serverId,
          saslProperties, callback);
    }
    if (saslServer == null) {
      throw new AccessControlException(
          "Unable to find SASL server implementation for " + mechanism);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Created SASL server with mechanism = " + mechanism);
    }
    return saslServer;
  }

  private static void initTauthSecureService(AuthMethod authMethod, UserGroupInformation ugi) throws IOException{
    if(authMethod == AuthMethod.TAUTH && secureServiceV2 == null) {
      synchronized(SaslRpcServer.class) {
        if(secureServiceV2 == null) {
          //Load smk first, otherwise using current login user
          LocalKeyManager localKeyManager = LocalKeyManager.generateByService(ENVUtils.CURUSER);
          serviceName = localKeyManager.getKeyLoader().getSubject();

          if(!localKeyManager.hasAnyKey()) {
            TAuthLoginModule.TAuthPrincipal principal  = TAuthLoginModule.TAuthPrincipal.getFromSubject(ugi.getSubject());
            TAuthLoginModule.TAuthCredential credential = TAuthLoginModule.TAuthCredential.getFromSubject(ugi.getSubject());
            if ((credential != null && credential.getLocalKeyManager().hasAnyKey())) {
              if(LOG.isDebugEnabled()) {
                LOG.debug("Using local key manager from current login user: " + principal);
              }
              serviceName = principal.getName();
              localKeyManager =  credential.getLocalKeyManager();
            }
          }

          if(!localKeyManager.hasAnyKey()) {
            throw new IOException("No credential found for tauth user " + ENVUtils.CURUSER);
          }

          LOG.info("Secure service initialized with user {}", serviceName);
          secureServiceV2 = SecurityCenterProvider.createTauthSecureService(
              serviceName, null, localKeyManager, true);
        }
      }
    }
  }

  public static void init(Configuration conf) {
    if (saslFactory == null) {
      Security.addProvider(new SaslPlainServer.SecurityProvider());
      Security.addProvider(new TqServerSecurityProvider());
      // passing null so factory is populated with all possibilities.  the
      // properties passed when instantiating a server are what really matter
      saslFactory = new FastSaslServerFactory(null);
    }
  }
  
  static String encodeIdentifier(byte[] identifier) {
    return new String(Base64.encodeBase64(identifier), StandardCharsets.UTF_8);
  }

  static byte[] decodeIdentifier(String identifier) {
    return Base64.decodeBase64(identifier.getBytes(StandardCharsets.UTF_8));
  }

  public static <T extends TokenIdentifier> T getIdentifier(String id,
      SecretManager<T> secretManager) throws InvalidToken {
    byte[] tokenId = decodeIdentifier(id);
    T tokenIdentifier;
    try {
      byte[] header = new byte[TqTokenIdentifier.TQ_TOKEN.getLength()];
      System.arraycopy(tokenId, 0, header, 0, Math.min(tokenId.length, header.length));
      // Tq token
      if (Arrays.equals(header, TqTokenIdentifier.TQ_TOKEN.copyBytes())) {
        TqTokenIdentifier tqTokenIdentifier = new TqTokenIdentifier();
        DataInput in = new DataInputStream(new BufferedInputStream(
            new ByteArrayInputStream(tokenId)));
        tqTokenIdentifier.readFields(in);
        return (T) tqTokenIdentifier;
      }

      tokenIdentifier = secretManager.createIdentifier();

      tokenIdentifier.readFields(new DataInputStream(new ByteArrayInputStream(
          tokenId)));
    } catch (Exception e) {
      throw (InvalidToken) new InvalidToken(
          "Can't de-serialize tokenIdentifier").initCause(e);
    }
    return tokenIdentifier;
  }

  static char[] encodePassword(byte[] password) {
    return new String(Base64.encodeBase64(password),
                      StandardCharsets.UTF_8).toCharArray();
  }

  /** Splitting fully qualified Kerberos name into parts */
  public static String[] splitKerberosName(String fullName) {
    return fullName.split("[/@]");
  }

  /** Authentication method */
  @InterfaceStability.Evolving
  public enum AuthMethod {
    SIMPLE((byte) 80, ""),
    KERBEROS((byte) 81, "GSSAPI"),
    @Deprecated
    DIGEST((byte) 82, "DIGEST-MD5"),
    TOKEN((byte) 82, "DIGEST-MD5"),
    PLAIN((byte) 83, "PLAIN"),
    TAUTH((byte) 84, TqAuthConst.TAUTH);

    /** The code for this method. */
    public final byte code;
    public final String mechanismName;

    private AuthMethod(byte code, String mechanismName) { 
      this.code = code;
      this.mechanismName = mechanismName;
    }

    private static final int FIRST_CODE = values()[0].code;

    /** Return the object represented by the code. */
    private static AuthMethod valueOf(byte code) {
      final int i = (code & 0xff) - FIRST_CODE;
      return i < 0 || i >= values().length ? null : values()[i];
    }

    /** Return the SASL mechanism name */
    public String getMechanismName() {
      return mechanismName;
    }

    /** Read from in */
    public static AuthMethod read(DataInput in) throws IOException {
      return valueOf(in.readByte());
    }

    /** Write to out */
    public void write(DataOutput out) throws IOException {
      out.write(code);
    }
  };

  /** CallbackHandler for SASL DIGEST-MD5 mechanism */
  @InterfaceStability.Evolving
  public static class SaslDigestCallbackHandler implements CallbackHandler {
    private SecretManager<TokenIdentifier> secretManager;
    private Server.Connection connection; 
    
    public SaslDigestCallbackHandler(
        SecretManager<TokenIdentifier> secretManager,
        Server.Connection connection) {
      this.secretManager = secretManager;
      this.connection = connection;
    }

    private char[] getPassword(TokenIdentifier tokenid) throws InvalidToken,
        StandbyException, RetriableException, IOException {
      if (TqTokenIdentifier.TQ_TOKEN.equals(tokenid.getKind())) {
        TqTokenIdentifier tqTokenIdentifier = (TqTokenIdentifier) tokenid;
        if (AuthConfigureHolder.isTqTokenEnable()) {
          try {
            SecureService secureService = SecureServiceFactory.getDefault();
            secureService.authenticate(Authentication.valueOf(tqTokenIdentifier.getAuthentication()));
          } catch (Exception e) {
            throw new InvalidToken("TQ token is invalid : " + e.getMessage());
          }
        }
        return encodePassword(tqTokenIdentifier.getPassword());
      }
      return encodePassword(secretManager.retriableRetrievePassword(tokenid));
    }

    @Override
    public void handle(Callback[] callbacks) throws InvalidToken,
        UnsupportedCallbackException, StandbyException, RetriableException,
        IOException {
      NameCallback nc = null;
      PasswordCallback pc = null;
      AuthorizeCallback ac = null;
      for (Callback callback : callbacks) {
        if (callback instanceof AuthorizeCallback) {
          ac = (AuthorizeCallback) callback;
        } else if (callback instanceof NameCallback) {
          nc = (NameCallback) callback;
        } else if (callback instanceof PasswordCallback) {
          pc = (PasswordCallback) callback;
        } else if (callback instanceof RealmCallback) {
          continue; // realm is ignored
        } else {
          throw new UnsupportedCallbackException(callback,
              "Unrecognized SASL DIGEST-MD5 Callback");
        }
      }
      if (pc != null) {
        // compatible support
        // TODO need to removed if version upgrade
        String[] names = nc.getDefaultName().split(TAuthConst.TAUTH_SPLITTER);
        String identifier = names[0];
        long id = -1;
        if (names.length > 1) {
          id = Long.valueOf(names[1]);
        }
        TokenIdentifier tokenIdentifier = getIdentifier(identifier,
            secretManager);
        if( tokenIdentifier instanceof AbstractDelegationTokenIdentifier) {
          AbstractDelegationTokenIdentifier delegationTokenIdent = (AbstractDelegationTokenIdentifier) tokenIdentifier;
          if(!delegationTokenIdent.isUnion()){
            delegationTokenIdent.setId(id);
          }
        }
        char[] password = getPassword(tokenIdentifier);
        UserGroupInformation user = null;
        user = tokenIdentifier.getUser(); // may throw exception
        connection.attemptingUser = user;
        
        if (LOG.isDebugEnabled()) {
          LOG.debug("SASL server DIGEST-MD5 callback: setting password "
              + "for client: " + tokenIdentifier.getUser());
        }
        pc.setPassword(password);
      }
      if (ac != null) {
        String authid = ac.getAuthenticationID();
        String authzid = ac.getAuthorizationID();
        if (authid.equals(authzid)) {
          ac.setAuthorized(true);
        } else {
          ac.setAuthorized(false);
        }
        if (ac.isAuthorized()) {
          if (LOG.isDebugEnabled()) {
            UserGroupInformation logUser =
              getIdentifier(authzid, secretManager).getUser();
            String username = logUser == null ? null : logUser.getUserName();
            LOG.debug("SASL server DIGEST-MD5 callback: setting "
                + "canonicalized client ID: " + username);
          }
          ac.setAuthorizedID(authzid);
        }
      }
    }
  }

  /** CallbackHandler for SASL GSSAPI Kerberos mechanism */
  @InterfaceStability.Evolving
  public static class SaslGssCallbackHandler implements CallbackHandler {

    @Override
    public void handle(Callback[] callbacks) throws
        UnsupportedCallbackException {
      AuthorizeCallback ac = null;
      for (Callback callback : callbacks) {
        if (callback instanceof AuthorizeCallback) {
          ac = (AuthorizeCallback) callback;
        } else {
          throw new UnsupportedCallbackException(callback,
              "Unrecognized SASL GSSAPI Callback");
        }
      }
      if (ac != null) {
        String authid = ac.getAuthenticationID();
        String authzid = ac.getAuthorizationID();
        if (authid.equals(authzid)) {
          ac.setAuthorized(true);
        } else {
          ac.setAuthorized(false);
        }
        if (ac.isAuthorized()) {
          if (LOG.isDebugEnabled())
            LOG.debug("SASL server GSSAPI callback: setting "
                + "canonicalized client ID: " + authzid);
          ac.setAuthorizedID(authzid);
        }
      }
    }
  }
  
  // Sasl.createSaslServer is 100-200X slower than caching the factories!
  private static class FastSaslServerFactory implements SaslServerFactory {
    private final Map<String,List<SaslServerFactory>> factoryCache =
        new HashMap<String,List<SaslServerFactory>>();

    FastSaslServerFactory(Map<String,?> props) {
      final Enumeration<SaslServerFactory> factories =
          Sasl.getSaslServerFactories();
      while (factories.hasMoreElements()) {
        SaslServerFactory factory = factories.nextElement();
        for (String mech : factory.getMechanismNames(props)) {
          if (!factoryCache.containsKey(mech)) {
            factoryCache.put(mech, new ArrayList<SaslServerFactory>());
          }
          factoryCache.get(mech).add(factory);
        }
      }
    }

    @Override
    public SaslServer createSaslServer(String mechanism, String protocol,
        String serverName, Map<String,?> props, CallbackHandler cbh)
        throws SaslException {
      SaslServer saslServer = null;
      List<SaslServerFactory> factories = factoryCache.get(mechanism);
      if (factories != null) {
        for (SaslServerFactory factory : factories) {
          saslServer = factory.createSaslServer(
              mechanism, protocol, serverName, props, cbh);
          if (saslServer != null) {
            break;
          }
        }
      }
      return saslServer;
    }

    @Override
    public String[] getMechanismNames(Map<String, ?> props) {
      return factoryCache.keySet().toArray(new String[0]);
    }
  }

  private static class TqAuthServerCallbackHandler extends TqServerCallbackHandler {
    private String serverName;
    private SecureServiceV2<TAuthAuthentication> secureService;

    public TqAuthServerCallbackHandler(String serverName, SecureServiceV2<TAuthAuthentication> secureService) {
      this.serverName = serverName;
      this.secureService = secureService;
    }

    @Override
    protected boolean isNeedAuth(byte[] extraId) {
      return AuthConfigureHolder.isAuthEnable()
          && (extraId == null
          || AuthConfigureHolder.getProtocolPolicyManagement().isNeedAuth(new String(extraId)));
    }

    @Override
    protected boolean isForbidden(String userName) {
      return AuthConfigureHolder.isNotAllow(userName);
    }

    @Override
    protected Tuple<byte[], String> processResponse(byte[] response) throws Exception {
      TqTicketResponseToken token = TqTicketResponseToken.valueOf(response);
      TAuthAuthentication tAuthAuthentication = new TAuthAuthentication(token.getSmkId(),
          token.getServiceTicket(), token.getAuthenticator());
      Authenticator authenticator = secureService.authenticate(tAuthAuthentication);
      return Tuple.of(authenticator.getSessionKey(), authenticator.getAuthUser());
    }

    @Override
    protected String getServiceName() {
      return serverName;
    }
  }
}
