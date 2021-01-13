package org.apache.hadoop.security;

import com.tencent.tdw.security.authentication.Authentication;
import com.tencent.tdw.security.utils.JacksonSerializer;
import com.tencent.tdw.security.utils.PropertiesUtils;
import com.tencent.tdw.security.utils.RestUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TqRestGroupMappingServiceProviderV2 extends TqRestGroupMappingServiceProvider {
  private static final Logger LOG = LoggerFactory.getLogger(TqRestGroupMappingServiceProviderV2.class);

  private static final String TQ_SECURITY_GROUP_UPDATE_DELAY_SECOND = "tq.security.group.update.delay.second";
  private static final String TQ_SECURITY_GROUP_UPDATE_INTERVAL_SECOND = "tq.security.group.update.interval.second";
  private static final String TQ_SECURITY_GROUP_REMOTE_FETCH_ENABLE = "tq.security.group.remote.fetch.enable";
  private volatile Map<String, List<String>> userGroups;

  private boolean remoteFetchEnable;
  private ScheduledExecutorService executorService;

  public TqRestGroupMappingServiceProviderV2() {
    super();

    userGroups = new HashMap<>();

    remoteFetchEnable = PropertiesUtils.getPropertyValueOrDefault(
        TQ_SECURITY_GROUP_REMOTE_FETCH_ENABLE, false);
    initGroupRefresher();
  }

  private void initGroupRefresher() {
    int groupUpdateDelay = PropertiesUtils.getPropertyValueOrDefault(
        TQ_SECURITY_GROUP_UPDATE_DELAY_SECOND, 1);
    int groupUpdateInterval = PropertiesUtils.getPropertyValueOrDefault(
        TQ_SECURITY_GROUP_UPDATE_INTERVAL_SECOND, 10 * 60);

    executorService = Executors.newSingleThreadScheduledExecutor();
    executorService.scheduleAtFixedRate(() -> {
      try {
        Map<String, List<String>> groups = RestUtil.getAllUserGroups(cluster);
        if (groups != null && groups.size() > 0) {
          userGroups = groups;
          LOG.info("Update {} user groups by cluster {}.", groups.size(), cluster);
        }
        if(LOG.isDebugEnabled()) {
          LOG.debug("Groups: " + JacksonSerializer.get().obj2Json(userGroups));
        }
      }catch (Exception ex) {
        LOG.error("Failed update groups:", ex);
      }
    }, groupUpdateDelay, groupUpdateInterval, TimeUnit.SECONDS);
  }

  private void ensureUserGroupsLoaded() {
    if(userGroups == null) {
      synchronized (this) {
        if(userGroups == null) {
          userGroups = RestUtil.getAllUserGroups(cluster);
          LOG.info("{} user groups loaded for cluster {}",
              userGroups != null ? userGroups.size() : 0, cluster);
        }
      }
    }
  }

  @Override
  public List<String> getGroups(String user) throws IOException {
    ensureUserGroupsLoaded();
    List<String> groups = userGroups.get(Authentication.getShortUserName(user));
    if(groups != null) {
      return groups;
    }

    if(remoteFetchEnable) {
      return super.getGroups(user);
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
