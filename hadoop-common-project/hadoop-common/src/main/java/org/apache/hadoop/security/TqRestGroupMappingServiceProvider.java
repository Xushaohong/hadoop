package org.apache.hadoop.security;

import com.tencent.tdw.security.authentication.service.SecureService;
import com.tencent.tdw.security.authentication.service.SecureServiceFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class TqRestGroupMappingServiceProvider implements GroupMappingServiceProvider, Configurable {
  private static final Logger LOG = LoggerFactory.getLogger(TqRestGroupMappingServiceProvider.class);
  private Configuration conf;
  private SecureService secureService;
  private GroupMappingServiceProvider fallback = new ShellBasedUnixGroupsMapping();

  @Override
  public List<String> getGroups(String user) throws IOException {
    try {
      if (secureService != null) {
        return secureService.getGroups(user);
      }

      return fallback.getGroups(user);
    } catch (Exception e) {
      LOG.warn("Failed to get group for " + user + ", by " + e.getMessage());
    }
    return Collections.emptyList();
  }

  @Override
  public void cacheGroupsRefresh() throws IOException {

  }

  @Override
  public void cacheGroupsAdd(List<String> groups) throws IOException {

  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    try {
      secureService = SecureServiceFactory.getDefault();
    } catch (Exception e) {
      LOG.warn("Initialization failed", e);
    }
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
