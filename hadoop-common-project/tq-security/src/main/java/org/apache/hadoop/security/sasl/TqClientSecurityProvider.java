package org.apache.hadoop.security.sasl;

import java.security.Provider;

public class TqClientSecurityProvider extends Provider {

  public TqClientSecurityProvider() {
    super("TqSaslClient", TqAuthConst.VERSION, "SASL TQ Authentication Client");
    put("SaslClientFactory." + TqAuthConst.TAUTH,
        TqFactoryImpl.class.getName());
  }
}
