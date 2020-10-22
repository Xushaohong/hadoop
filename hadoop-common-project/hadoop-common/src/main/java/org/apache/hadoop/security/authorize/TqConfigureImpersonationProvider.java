package org.apache.hadoop.security.authorize;

import com.tencent.tdw.security.authentication.ImpersonationProvider;
import com.tencent.tdw.security.authentication.service.SecureServiceFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;

import java.util.List;

public class TqConfigureImpersonationProvider extends DefaultImpersonationProvider {
  private static final Log LOG = LogFactory.getLog(TqConfigureImpersonationProvider.class);
  private ImpersonationProvider impersonationProvider;
  private boolean initialized = false;
  private boolean enable;
  private boolean fallback;


  @Override
  public void init(String configurationPrefix) {
    super.init(configurationPrefix);
    this.enable = getConf().getBoolean("tq.impersonation.enable",
        true);
    this.fallback = getConf().getBoolean("tq.impersonation.fallback.enable",
        true);
    try {
      this.impersonationProvider =
          SecureServiceFactory.getImpersonationProvider(SecureServiceFactory.getDefault());
      this.impersonationProvider.start();
      this.initialized = true;
    } catch (Exception e) {
      LOG.error("Failed to init..", e);
    }
  }

  @Override
  public void authorize(UserGroupInformation user, String remoteAddress) throws AuthorizationException {
    if (initialized && enable) {
      UserGroupInformation realUser = user.getRealUser();
      if (realUser == null) {
        return;
      }
      List<String> groups = user.getGroups();
      String realUserName = realUser.getShortUserName();
      String userName = user.getShortUserName();
      try {
        impersonationProvider.authorize(realUserName, userName, groups, remoteAddress);
        return;
      } catch (com.tencent.tdw.security.authentication.AuthorizationException e) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("Authorization failed.(%s : %s)", realUserName, userName), e);
        }
        if (!fallback) {
          throw new AuthorizationException(e);
        }
      }
    }
    super.authorize(user, remoteAddress);
  }
}
