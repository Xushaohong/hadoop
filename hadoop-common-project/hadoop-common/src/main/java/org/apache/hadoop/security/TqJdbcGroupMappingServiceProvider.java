package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class TqJdbcGroupMappingServiceProvider implements GroupMappingServiceProvider, Configurable {
  private static final Logger LOG = LoggerFactory.getLogger(TqJdbcGroupMappingServiceProvider.class);

  private Configuration conf;
  private JdbcGroupProvider jdbcGroupProvider = new JdbcGroupProvider();

  @Override
  public List<String> getGroups(String user) throws IOException {
    if (jdbcGroupProvider.isInitialized()) {
      try {
        return jdbcGroupProvider.doGetGroups(user);
      } catch (Exception e) {
        LOG.warn("Failed to get group for " + user + ", by " + e.getMessage());
      }
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
    String url = conf.get("tq.security.permissiondb.url", "jdbc:postgresql://localhost:5432/db");
    String table = conf.get("tq.security.permissiondb.role.table", "tdwuserrole");
    String user = conf.get("tq.security.permissiondb.user", "user");
    String password = conf.get("tq.security.permissiondb.password", "password");
    int maxCon = conf.getInt("tq.security.permissiondb.connection.max", 20);
    int minCon = conf.getInt("tq.security.permissiondb.connection.min", 2);
    jdbcGroupProvider.initialize(url, table, user, password, maxCon, minCon);
    LOG.info(jdbcGroupProvider + " is initialized");
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
