package org.apache.hadoop.security;

import com.tencent.tdw.security.utils.RestUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class TqRestGroupMappingServiceProvider implements GroupMappingServiceProvider {
  private static final Logger LOG = LoggerFactory.getLogger(TqRestGroupMappingServiceProvider.class);
  private GroupMappingServiceProvider fallback = new ShellBasedUnixGroupsMapping();

  @Override
  public List<String> getGroups(String user) throws IOException {
    try {
      return RestUtil.getGroups(user);
    } catch (Exception e) {
      LOG.warn("Failed to get group for " + user + ", by " + e.getMessage());
    }
    try {
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
}
