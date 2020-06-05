package org.apache.hadoop.security.tauth;

import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslClientFactory;
import javax.security.sasl.SaslServer;
import javax.security.sasl.SaslServerFactory;
import java.util.Map;

public class TAuthFactory implements SaslClientFactory, SaslServerFactory {

  @Override
  public SaslClient createSaslClient(String[] mechanisms, String authorizationId, String protocol,
      String serverName, Map<String, ?> props, CallbackHandler cbh) {
    for (String mechanism : mechanisms) {
      if (TAuthConst.TAUTH.equals(mechanism)) {
        return new TAuthSaslClient(protocol, serverName, props, cbh);
      }
    }
    return null;
  }

  @Override
  public SaslServer createSaslServer(String mechanism, String protocol, String serverName,
      Map<String, ?> props, CallbackHandler cbh) {
    if (TAuthConst.TAUTH.equals(mechanism)) {
      return new TAuthSaslServer(protocol, serverName, props, cbh);
    }
    return null;
  }

  @Override
  public String[] getMechanismNames(Map<String, ?> props) {
    return new String[]{TAuthConst.TAUTH};
  }
}
