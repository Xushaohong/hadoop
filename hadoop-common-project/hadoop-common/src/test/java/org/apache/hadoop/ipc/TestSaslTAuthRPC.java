package org.apache.hadoop.ipc;

import com.google.common.collect.Lists;
import com.tencent.tdw.security.authentication.LocalKeyManager;
import com.tencent.tdw.security.authentication.SecretKeyProvider;
import com.tencent.tdw.security.authentication.SecurityCenterProvider;
import com.tencent.tdw.security.minicluster.LocalSecurityCenter;
import com.tencent.tdw.security.minicluster.SecurityController;
import com.tencent.tdw.security.netbeans.SecretKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.SaslInputStream;
import org.apache.hadoop.security.SaslRpcClient;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.BypassImpersonationProvider;
import org.apache.hadoop.security.authorize.ImpersonationProvider;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import javax.security.sasl.SaslException;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import static com.tencent.tdw.security.utils.TdwConst.SECURITY_VALIDATE_KEY_EXCLUSIVE_PERMISSION;
import static com.tencent.tdw.security.utils.TdwConst.SECURITY_VALIDATE_KEY_OWNER;
import static com.tencent.tdw.security.utils.TdwConst.TDW_SECURITY_KEY_BASE_PATH;

/**
 * 测试用例：
 *    1. tauth 认证
 *    2. tauth 黑白名单
 *    3. tauth fallback to simple
 *    4. tauth 禁用
 *    5. tauth 代理
 */
public class TestSaslTAuthRPC extends TestRpcBase {
  public static final Logger LOG = LoggerFactory.getLogger(TestSaslTAuthRPC.class);

  static {
    GenericTestUtils.setLogLevel(Client.LOG, Level.TRACE);
    GenericTestUtils.setLogLevel(Server.LOG, Level.TRACE);
    GenericTestUtils.setLogLevel(SaslRpcClient.LOG, Level.TRACE);
    GenericTestUtils.setLogLevel(SaslRpcServer.LOG, Level.TRACE);
    GenericTestUtils.setLogLevel(SaslInputStream.LOG, Level.TRACE);
    GenericTestUtils.setLogLevel(SecurityUtil.LOG, Level.TRACE);
  }

  private static String tauthBaseDir;
  private static LocalSecurityCenter securityCenter;

  @BeforeClass
  public static void setup() throws IOException {
    LOG.info("---------------------------------");
    LOG.info("Testing TAUTH");
    LOG.info("---------------------------------");

    //设置TAUTH测试环境，包括tauth文件目录，不检查文件owner，不检查文件权限
    System.setProperty(SECURITY_VALIDATE_KEY_EXCLUSIVE_PERMISSION, Boolean.toString(false));
    System.setProperty(SECURITY_VALIDATE_KEY_OWNER, Boolean.toString(false));
    tauthBaseDir = TestSaslTAuthRPC.class.getResource("/").getPath() + "secure";
    System.setProperty(TDW_SECURITY_KEY_BASE_PATH, tauthBaseDir);

    conf = new Configuration();
    // the specific tests for kerberos will enable kerberos.  forcing it
    // for all tests will cause tests to fail if the user has a TGT
    SecurityUtil.setAuthenticationMethod(
        UserGroupInformation.AuthenticationMethod.TAUTH, conf);
    UserGroupInformation.setConfiguration(conf);

    // Set RPC engine to protobuf RPC engine
    RPC.setProtocolEngine(conf, TestRpcService.class, ProtobufRpcEngine.class);

    System.setProperty(SecurityCenterProvider.SECURITY_CENTER_CLASS_KEY, LocalSecurityCenter.class.getName());
    securityCenter = (LocalSecurityCenter)SecurityCenterProvider.getSecurityCenter();
  }

  @Before
  public void setupTauth() throws IOException {
    FileSystem localFilesystem = FileSystem.getLocal(conf);
    localFilesystem.delete(new Path(tauthBaseDir), true);
  }

  @Test
  public void testTauthLogin() throws Exception {
    FileSystem localFilesystem = FileSystem.getLocal(conf);
    String localUser = UserGroupInformation.getCurrentUser().getUserName();

    UserGroupInformation.getCurrentUser().logoutUserFromTAuth();
    UserGroupInformation.setLoginUser(null);

    // ./testUser/.tdw/.cmk/ 目录加载秘钥
    String keyPath = SecretKeyProvider.getKeyPath(localUser, true);

    localFilesystem.mkdirs(new Path(keyPath));
    Path cmkKeyFile = new Path(keyPath, localUser + "-0101");
    LOG.info("Cmk key file: " + cmkKeyFile);
    String cmkString = "{\"id\":240,\"version\":1,\"subject\":\"hdfsClient\",\"key\":\"abcdefg\",\"type\":\"cmk\",\"createTime\":1571905080329}";
    FileUtil.write(localFilesystem, cmkKeyFile, cmkString);
    UserGroupInformation ugi = UserGroupInformation.getLoginUser();
    Assert.assertEquals("hdfsClient", ugi.getUserName());

    UserGroupInformation.getCurrentUser().logoutUserFromTAuth();
    UserGroupInformation.setLoginUser(null);
    localFilesystem.delete(new Path(keyPath), true);

    String smkString = "{\"id\":240,\"version\":1,\"subject\":\"hdfsService\",\"key\":\"abcdefg\",\"type\":\"cmk\",\"createTime\":1571905080329}";

    // ./testUser/.tdw/.smk/ 目录加载秘钥
    keyPath = SecretKeyProvider.getKeyPath(localUser, false);
    localFilesystem.mkdirs(new Path(keyPath));
    Path smkKeyFile = new Path(keyPath, localUser + "-0101");
    LOG.info("Smk key file: " + smkKeyFile);
    FileUtil.write(localFilesystem, smkKeyFile, smkString);
    LocalKeyManager localKeyManager = LocalKeyManager.generateByService(localUser);
    Assert.assertEquals("hdfsService", localKeyManager.getKeyLoader().getSubject());
    localFilesystem.delete(new Path(keyPath), true);

    // ./testUser/.tdw/ 目录加载秘钥
    keyPath = new Path(SecretKeyProvider.getKeyPath(localUser, false)).getParent().toString();
    localFilesystem.mkdirs(new Path(keyPath));
    smkKeyFile = new Path(keyPath, localUser + "-0101");
    LOG.info("Smk key file: " + smkKeyFile);
    FileUtil.write(localFilesystem, smkKeyFile, smkString);
    ugi = UserGroupInformation.getLoginUser();
    Assert.assertEquals("hdfsService", ugi.getUserName());
  }

  @Test
  public void testTauthRpc() throws Exception {
    final Configuration newConf = new Configuration(conf);

    String clientName = "client";
    String serviceName = "hdfs";
    SecurityController securityController = securityCenter.getController();
    SecretKey clientKey = securityController.createUser(clientName);
    SecretKey serviceKey = securityController.createService(serviceName, "");

    UserGroupInformation serverUser = UserGroupInformation.createUserByTAuthKey(serviceName, serviceKey.getKey());
    UserGroupInformation.setLoginUser(serverUser);
    Server server = setupTestServer(newConf, 5);

    UserGroupInformation clientUser = UserGroupInformation.createUserByTAuthKey(clientName, clientKey.getKey());
    clientUser.doAs((PrivilegedExceptionAction<Object>) () -> {
      TestRpcService proxy = null;
      try {
        proxy = getClient(addr, newConf);
        proxy.ping(null, newEmptyRequest());
        return null;
      }finally {
        stop(server, proxy);
      }
    });
  }


  @Test
  public void testTauthProxyRpc() throws Exception {
    final Configuration newConf = new Configuration(conf);

    //Skip proxy check by DefaultImpersonationProvider
    newConf.setClass(CommonConfigurationKeysPublic.HADOOP_SECURITY_IMPERSONATION_PROVIDER_CLASS,
        BypassImpersonationProvider.class, ImpersonationProvider.class);
    ProxyUsers.refreshSuperUserGroupsConfiguration(newConf);

    String clientName = "client";
    String serviceName = "hdfs";
    String proxyName = "proxyUser";
    SecurityController securityController = securityCenter.getController();
    SecretKey clientKey = securityController.createUser(clientName);
    SecretKey serviceKey = securityController.createService(serviceName, "");

    UserGroupInformation serverUser = UserGroupInformation.createUserByTAuthKey(serviceName, serviceKey.getKey());
    UserGroupInformation.setLoginUser(serverUser);
    Server server = setupTestServer(newConf, 5);

    UserGroupInformation clientUser = UserGroupInformation.createUserByTAuthKey(clientName, clientKey.getKey());
    UserGroupInformation proxyUser = UserGroupInformation.createProxyUser(proxyName, clientUser);
    proxyUser.doAs((PrivilegedExceptionAction<Object>) () -> {
      TestRpcService  proxy = getClient(addr, newConf);
      try {
        proxy.ping(null, newEmptyRequest());
        Assert.fail("User " + clientName + " should not impersonate " + proxyName);
      }catch (Exception ex) {
        Assert.assertTrue(ex.getCause() instanceof RemoteException);
        RemoteException re = (RemoteException)ex.getCause();
        Assert.assertEquals(SaslException.class.getName(), re.getClassName());
        Assert.assertTrue(re.getMessage().contains("Auth failed"));
        Assert.assertTrue(re.getMessage().contains("is not allowed to impersonate"));
      }

      try {
        securityController.addImpersonation(clientName, Lists.newArrayList(proxyName), Lists.newArrayList("*"));
        proxy = getClient(addr, newConf);
        proxy.ping(null, newEmptyRequest());
      }finally {
        stop(server, proxy);
      }
      return null;
    });
  }
}