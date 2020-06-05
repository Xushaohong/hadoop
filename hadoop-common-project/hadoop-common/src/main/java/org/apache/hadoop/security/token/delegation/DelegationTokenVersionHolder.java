package org.apache.hadoop.security.token.delegation;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier.UNION_DELEGATION_TOKEN_IDENTIFIER_VERSION;

//TODO Move to AbstractDelegationTokenIdentifier when all cluster upgrade
public class DelegationTokenVersionHolder {
  private static final Logger LOG = LoggerFactory.getLogger(DelegationTokenVersionHolder.class);

  //for bytes of identifier
  public static byte TOKEN_IDENTIFIER_TRANSMISSION_VERSION = UNION_DELEGATION_TOKEN_IDENTIFIER_VERSION;

  //for image serialize.( avoid standby unsupported )
  public static byte TOKEN_IDENTIFIER_SERIALIZATION_VERSION = TOKEN_IDENTIFIER_TRANSMISSION_VERSION;

  static {
    refresh();
  }

  public static void refresh() {
    Configuration conf = new Configuration();
    TOKEN_IDENTIFIER_TRANSMISSION_VERSION = (byte) conf.getInt(
        "delegation.token.identifier.transmission.version", UNION_DELEGATION_TOKEN_IDENTIFIER_VERSION);
    TOKEN_IDENTIFIER_SERIALIZATION_VERSION = (byte) conf.getInt(
        "delegation.token.identifier.serialization.version", TOKEN_IDENTIFIER_TRANSMISSION_VERSION);
    LOG.info("TOKEN_IDENTIFIER_TRANSMISSION_VERSION:" + TOKEN_IDENTIFIER_TRANSMISSION_VERSION);
    LOG.info("TOKEN_IDENTIFIER_SERIALIZATION_VERSION:" + TOKEN_IDENTIFIER_SERIALIZATION_VERSION);
  }
}