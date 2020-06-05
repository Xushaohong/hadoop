package org.apache.hadoop.security.sasl;

import java.security.Provider;

public class TqServerSecurityProvider extends Provider {

    public TqServerSecurityProvider() {
      super("TQSaslServer", TqAuthConst.VERSION, "SASL TQ Authentication Server");
      put("SaslServerFactory." + TqAuthConst.TAUTH,
          TqFactoryImpl.class.getName());
    }
  }