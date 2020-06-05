package org.apache.hadoop.security;

import com.tencent.tdw.security.authentication.Authentication;
import com.tencent.tdw.security.authentication.UserGroups;
import com.tencent.tdw.security.utils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

public class JdbcGroupProvider {
  private static Logger LOG = LoggerFactory.getLogger(JdbcGroupProvider.class);

  private static final String USER_NAME = "user_name";
  private static final String ROLE_NAME = "role_name";
  private String url;
  private String roleTable;
  private ConnectionPool connPool;
  private final static String QUERY_SQL_FORMAT = "select %s from %s %s";
  private String queryUserGroupSql;
  private String queryAllUserGroupSql;
  private boolean initialized;

  public JdbcGroupProvider() {

  }

  public void initialize(String url, String roleTable, String user, String password,
      int maxConnection, int minConnection) {
    this.url = url;
    this.roleTable = roleTable;
    this.connPool = new ConnectionPool(ConnectionPool.buildPoolFactory(url, user, password),
        ConnectionPool.buildPoolConfig(maxConnection, minConnection));
    this.queryUserGroupSql = String.format(QUERY_SQL_FORMAT, ROLE_NAME, roleTable,
        "where " + USER_NAME + "=? or " + USER_NAME + "=?");
    this.queryAllUserGroupSql = String.format(QUERY_SQL_FORMAT, USER_NAME + "," + ROLE_NAME, roleTable,
        "where " + "1=1");
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        close();
      }
    });
    this.initialized = true;
  }

  private void close() {
    if (this.connPool != null) {
      connPool.close();
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Destroy connection pool");
    }
  }

  public List<String> doGetGroups(final String userName) {
    return apply(new Function<Connection, List<String>>() {
      @Override
      public List<String> apply(final Connection connection) {
        return Try.apply(new Action<List<String>>() {
          @Override
          public List<String> apply() throws Exception {
            try (PreparedStatement preparedStatement = connection.prepareStatement(queryUserGroupSql)) {
              preparedStatement.setString(1, userName);
              preparedStatement.setString(2, TdwConst.TDW_PREFIX + userName);
              try (ResultSet resultSet = preparedStatement.executeQuery()) {
                List<String> userGroups = new ArrayList<>();
                while (resultSet.next()) {
                  String group = resultSet.getString(ROLE_NAME);
                  if (group == null) {
                    LOG.warn("Group is empty(" + userName + ":" + group + ")");
                    continue;
                  }
                  userGroups.add(group);
                }
                return userGroups;
              }
            }
          }
        }, new Function<Exception, List<String>>() {
          @Override
          public List<String> apply(Exception e) {
            LOG.error("Failed to get user(" + userName + ")'s groups:" + e.getMessage());
            return Collections.emptyList();
          }
        });
      }
    });
  }

  public List<UserGroups> doGetUserGroups() {
    return apply(new Function<Connection, List<UserGroups>>() {
      @Override
      public List<UserGroups> apply(final Connection connection) {
        return Try.apply(new Action<List<UserGroups>>() {
          @Override
          public List<UserGroups> apply() throws Exception {
            try (PreparedStatement preparedStatement = connection.prepareStatement(queryAllUserGroupSql)) {
              try (ResultSet resultSet = preparedStatement.executeQuery()) {
                Map<String, List<String>> userMappingGroups = new HashMap<>();
                while (resultSet.next()) {
                  String userName = resultSet.getString(USER_NAME);
                  String group = resultSet.getString(ROLE_NAME);
                  if (userName == null || group == null) {
                    LOG.warn("User or group is empty(" + userName + ":" + group + ")");
                    continue;
                  }
                  // convert userName to lower case
                  userName = Authentication.getShortUserName(userName.toLowerCase());
                  List<String> groups = userMappingGroups.get(userName);
                  if (groups == null) {
                    groups = new ArrayList<>();
                    userMappingGroups.put(userName, groups);
                  }
                  groups.add(group);
                }
                List<UserGroups> userGroups = new ArrayList<>(userMappingGroups.size());
                for (Map.Entry<String, List<String>> entry : userMappingGroups.entrySet()) {
                  userGroups.add(new UserGroups(entry.getKey(), entry.getValue()));
                }
                return userGroups;
              }
            }
          }
        }, new Function<Exception, List<UserGroups>>() {
          @Override
          public List<UserGroups> apply(Exception e) {
            LOG.error("Failed to get user groups:" + e.getMessage());
            return Collections.emptyList();
          }
        });
      }
    });
  }

  public boolean isInitialized() {
    return initialized;
  }

  private <T> T apply(Function<Connection, T> function) {
    Connection conn = null;
    try {
      return function.apply(conn = Try.apply(new Action<Connection>() {
        @Override
        public Connection apply() throws Exception {
          return connPool.borrowObject();
        }
      }));
    } finally {
      if (conn != null) {
        connPool.returnObject(conn);
      }
    }
  }

  @Override
  public String toString() {
    return "JdbcGroupProvider{" +
        "url='" + url + '\'' +
        ", roleTable='" + roleTable + '\'' +
        '}';
  }
}
