package org.apache.hadoop.security.authorize;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BypassImpersonationProvider implements ImpersonationProvider {
  private static final Logger LOG = LoggerFactory.getLogger(BypassImpersonationProvider.class);

  @Override
  public void init(String configurationPrefix) {

  }

  @Override
  public void authorize(UserGroupInformation user, String remoteAddress) throws AuthorizationException {
    if(LOG.isDebugEnabled()) {
      LOG.debug("Bypass impersonation for " + user.toString());
    }
  }

  @Override
  public void setConf(Configuration conf) {

  }

  @Override
  public Configuration getConf() {
    return null;
  }
}
