package org.apache.hadoop.security.sasl;


import org.apache.hadoop.security.tauth.TAuthSaslClient;
import org.apache.hadoop.security.tauth.TAuthSaslServer;

import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.SaslClientFactory;
import javax.security.sasl.SaslServerFactory;
import java.util.Map;

import static org.apache.hadoop.security.sasl.TqAuthConst.TAUTH;

public class TqFactoryImpl implements SaslClientFactory, SaslServerFactory {


  @Override
  public javax.security.sasl.SaslClient createSaslClient(String[] mechanisms, String authorizationId, String protocol,
      String serverName, Map<String, ?> props, CallbackHandler cbh) {
    for (String mechanism : mechanisms) {
      if (TAUTH.equals(mechanism)) {
        if (cbh instanceof TqClientCallbackHandler) {
          return new TqSaslClient(authorizationId, protocol, serverName, props, cbh);
        }
        return new TAuthSaslClient(protocol, serverName, props, cbh);
      }
    }
    return null;
  }

  @Override
  public javax.security.sasl.SaslServer createSaslServer(String mechanism, String protocol, String serverName,
      Map<String, ?> props, CallbackHandler cbh) {
    if (TAUTH.equals(mechanism)) {
      if( cbh instanceof  TqServerCallbackHandler) {
        return new TqSaslServer(protocol, serverName, props, cbh);
      }
      return new TAuthSaslServer(protocol, serverName, props, cbh);
    }
    return null;
  }

  @Override
  public String[] getMechanismNames(Map<String, ?> props) {
    return new String[]{TAUTH};
  }
}
