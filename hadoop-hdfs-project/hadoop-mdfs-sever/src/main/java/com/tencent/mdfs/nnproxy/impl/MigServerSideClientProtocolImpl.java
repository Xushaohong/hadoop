package com.tencent.mdfs.nnproxy.impl;

import static com.tencent.mdfs.config.Constants.MIG_DFS_NEW_SCHEMA;
import static com.tencent.mdfs.config.Constants.MIG_DFS_OLD_SCHEMA;
import static com.tencent.mdfs.config.Constants.PCG_OLD_VERSION;
import static com.tencent.mdfs.nnproxy.security.RouterSecurityManager.getRemoteUser;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CHECKSUM_TYPE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CHECKSUM_TYPE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_ENCRYPT_DATA_TRANSFER_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_REPLICATION_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_CLIENT_WRITE_PACKET_SIZE_KEY;

import com.google.common.base.Strings;
import com.tencent.mdfs.entity.OpType;
import com.tencent.mdfs.entity.RouteInfo;
import com.tencent.mdfs.nnproxy.conn.ConnectionContext;
import com.tencent.mdfs.nnproxy.conn.ConnectionManager;
import com.tencent.mdfs.nnproxy.context.ContextUtil;
import com.tencent.mdfs.nnproxy.context.RpcContextUtils;
import com.tencent.mdfs.nnproxy.security.RouterSecurityManager;
import com.tencent.mdfs.router.RouteServiceManager;
import com.tencent.mdfs.router.RouteUtil;
import com.tencent.mdfs.util.ClientNnCacheUtil;
import com.tencent.mdfs.util.LocalCache;
import com.tencent.mdfs.util.MMUtil;
import com.tencent.mdfs.util.ThreadLocalUtil;
import com.tencent.mdfs.util.VnnConfigUtil;
import com.tencent.mdfs.util.VnnLogUtil;
import com.tencent.mdfs.util.VnnPathUtil;
import com.tencent.mdfs.exception.MigHadoopException;
import com.tencent.mdfs.exception.MigNotRetryableException;
import com.tencent.mdfs.exception.MigNotSupportProxyMethodException;
import com.tencent.mdfs.exception.MigPermissionDenyException;
import com.tencent.mdfs.exception.MigRouterException;
import com.tencent.mdfs.util.StringUtil;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.net.SocketFactory;

import org.apache.commons.collections.map.LRUMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.fs.BatchedRemoteIterator;
import org.apache.hadoop.fs.BatchedRemoteIterator.BatchedEntries;
import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.QuotaUsage;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.hdfs.AddBlockFlag;
import org.apache.hadoop.hdfs.NameNodeProxies;
import org.apache.hadoop.hdfs.NameNodeProxiesClient.ProxyAndInfo;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.inotify.EventBatchList;
import org.apache.hadoop.hdfs.protocol.AddErasureCodingPolicyResponse;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.CorruptFileBlocks;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.ECBlockGroupStats;
import org.apache.hadoop.hdfs.protocol.EncryptionZone;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicyInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.ReencryptAction;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsLocatedFileStatus;
import org.apache.hadoop.hdfs.protocol.LastBlockWithStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.OpenFileEntry;
import org.apache.hadoop.hdfs.protocol.OpenFilesIterator.OpenFilesType;
import org.apache.hadoop.hdfs.protocol.ReplicatedBlockStats;
import org.apache.hadoop.hdfs.protocol.RollingUpgradeInfo;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReportListing;
import org.apache.hadoop.hdfs.protocol.SnapshotStatus;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.apache.hadoop.hdfs.protocol.ZoneReencryptionStatus;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.namenode.UnsupportedActionException;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.RpcNoSuchMethodException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DataChecksum;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * create:chunxiaoli Date:2/6/18
 */
public class MigServerSideClientProtocolImpl implements ClientProtocol {

  private static final Logger LOGGER = LogManager.getLogger("server");
  private final ClientProtocol clientProtocol;
  private final Configuration conf;
  private final ConcurrentHashMap<String, ClientProtocol> oldClientCache = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, ClientProtocol> clientCache = new ConcurrentHashMap<>();
  public final RouterSecurityManager securityManager;
  private final ConnectionManager connectionManager;

  //The method without path parameter finds the nn address by clientname
  private final Map<String,String> clientNNMap = Collections.synchronizedMap(new LRUMap(100000));
  private final List<String> noProxyVersion = new ArrayList<>();


  public MigServerSideClientProtocolImpl(final Configuration conf, RouterSecurityManager securityManager, ConnectionManager connectionManager) throws IOException {
    this.conf = conf;
    this.clientProtocol = NameNodeProxies.createProxy(conf, FileSystem.getDefaultUri(conf),
        ClientProtocol.class).getProxy();
    this.securityManager = securityManager;
    this.connectionManager = connectionManager;
    String versionsStr = VnnConfigUtil.get(PCG_OLD_VERSION);
    if(versionsStr != null){
      noProxyVersion.addAll(Arrays.asList(versionsStr.split(",")));
    }

    VnnLogUtil.log("MigServerSideClientProtocolImpl init ok");

  }


  /**
   * chose write nnAddr for one rpc request
   * If it is 2.7.3, do not go for certification,
   * if it is 2.8.5, go for certification
   * @param routeInfo rout infomation
   * @param addr path address
   * @return
   */
  private ClientProtocol getClientProtocol(RouteInfo routeInfo, String addr) {

    if (StringUtils.isEmpty(addr)) {
      LOGGER.error("getClientProtocol  addr is null " + addr);
      throw new MigNotRetryableException("addr is null or empty ");
    }

    ClientProtocol ret = null;
    RpcContextUtils.setRealNNIp(addr);
    //这里需要动态，万一有升级的，不能继续用旧的链接
    if(isPcgOldHadoop(addr)){
      ret = oldClientCache.get(addr);
      if(ret != null){
        LOGGER.debug("get from old cache :{} ", addr);

        return ret;
      }
    }else{
      ret = clientCache.get(addr);
      if(ret != null){
        LOGGER.debug("get from new cache :{} ", addr);

        return ret;
      }
    }
    //这里不走代理，直接访问
    if (isPcgOldHadoop(addr)) {
      LOGGER.debug("is pcg old:{} ", addr);
      URI nameNodeUri = URI.create(HdfsConstants.HDFS_URI_SCHEME + "://" + addr);
      try {
        ret = NameNodeProxies.createProxy(conf, nameNodeUri, ClientProtocol.class).getProxy();
      } catch (IOException e) {
        e.printStackTrace();
        VnnLogUtil.err("NameNodeProxies.createProxy error" + e);
      }
      if (ret != null) {
        oldClientCache.put(addr, ret);
      }
      return ret;
    }
    LOGGER.debug("is not pcg old:{} ", addr);

    try {
      final RetryPolicy defaultPolicy = RetryUtils.getDefaultRetryPolicy(conf,
          HdfsClientConfigKeys.Retry.POLICY_ENABLED_KEY,
          HdfsClientConfigKeys.Retry.POLICY_ENABLED_DEFAULT,
          HdfsClientConfigKeys.Retry.POLICY_SPEC_KEY,
          HdfsClientConfigKeys.Retry.POLICY_SPEC_DEFAULT,
          HdfsConstants.SAFEMODE_EXCEPTION_CLASS_NAME);
      SocketFactory factory = SocketFactory.getDefault();
      if (UserGroupInformation.isSecurityEnabled()) {
        SaslRpcServer.init(conf);
      }
      InetSocketAddress socket = NetUtils.createSocketAddr(addr);
      UserGroupInformation ugi =  getRemoteUser();
      LOGGER.debug("remoteUser:"+ugi.getUserName());
      UserGroupInformation connUGI = ugi;
      //All requests proxied to outside the old pcg cluster
      // are forced to be specified as the user set by VNN_SET_FORCE_USER
      String forceUser =  System.getenv("VNN_SET_FORCE_USER");
      if (StringUtils.isNotBlank(forceUser)) {
        forceUser = StringUtils.deleteWhitespace(forceUser);
      }
      if (StringUtils.isNotBlank(forceUser)) {
        connUGI = UserGroupInformation.createRemoteUser(forceUser);
      } else {
        if (UserGroupInformation.isSecurityEnabled()) {
          UserGroupInformation routerUser = UserGroupInformation.getLoginUser();
          connUGI = UserGroupInformation.createProxyUser(
                  ugi.getUserName(), routerUser);
          LOGGER.debug("routerUser:"+routerUser.getUserName());
        }
      }
      ConnectionContext connection = this.connectionManager.getConnection(
              connUGI, addr, ClientProtocol.class);
      ProxyAndInfo<?> client = connection.getClient();
      final ClientProtocol proxy = (ClientProtocol)client.getProxy();
      return proxy;
//      ClientNamenodeProtocolPB proxy = RPC.getProtocolProxy(ClientNamenodeProtocolPB.class,
//          RPC.getProtocolVersion(ClientNamenodeProtocolPB.class), socket, connUGI,
//          conf, factory, RPC.getRpcTimeout(conf), defaultPolicy, null).getProxy();
//      ret = new ClientNamenodeProtocolTranslatorPB(proxy);

      //URI nameNodeUri = URI.create(HdfsConstants.HDFS_URI_SCHEME + "://" + addr);
      //ret = NameNodeProxies.createProxy(conf, nameNodeUri, ClientProtocol.class).getProxy();
    } catch (IOException e) {
      e.printStackTrace();
      VnnLogUtil.err("NameNodeProxies.createProxy error" + e);
    }
    //fixme
    return ret;
  }

  boolean isPcgOldHadoop(String addr){
    if(Strings.isNullOrEmpty(addr)){
      return false;
    }
    String version = null;
    if(addr.indexOf(":") > -1){
      String ip = addr.split(":")[0];
      //Which versions are read from the configuration without proxying
      version = LocalCache.nnVersion.get(ip);
      LOGGER.debug("version:{},{}",version,ip);
    }else {
      version = LocalCache.nnVersion.get(addr);
      LOGGER.debug("version:{},{}",version,addr);
    }

    if(LOGGER.isDebugEnabled()) {
      for (Map.Entry<String, String> entry : LocalCache.nnVersion.entrySet()) {
        LOGGER.debug("nnversion:{},{}", entry.getKey(), entry.getValue());
      }
    }
    return noProxyVersion.contains(version);
  }

  private RouteInfo checkPermissionAndGetRoute(final String src, OpType op,
      boolean... isAdminOperator) throws IOException {

    LOGGER.debug("checkPermissionAndGetRoute :{} {}", src, op);
    long start = System.currentTimeMillis();
    int status = 0;

    RouteInfo ret;

    //PermissionServiceManager.getInstance().checkPermission(new OpAction(src, op, adminOperator));
    if (src == null) {
      ret = RouteUtil.getDefaultRouteInfo();
      MMUtil.reportCheckPermissionAndGetRoute(start, status);
      ThreadLocalUtil.routeInfo.set(ret);
      LOGGER.debug("src is null ret :{} ", ret);
      return ret;
    }

    boolean isMDFS = VnnPathUtil.isMDFS(src);
    ThreadLocalUtil.src.set(src);
    try {
      ContextUtil.updateSchema(isMDFS ? MIG_DFS_NEW_SCHEMA : MIG_DFS_OLD_SCHEMA);
      ret = RouteServiceManager.getInstance().route(src);

      if (ret == null) {
        //todo
        VnnLogUtil.err("can not find route info for " + src + " " + ContextUtil.getClientInfo());
        throw new MigRouterException(src);
      }
    } catch (Exception e) {
      VnnLogUtil.err(ContextUtil.getClientInfo() + " checkPermissionAndGetRoute error " + e);
      status = 1;
      throw e;
    } finally {
      MMUtil.reportCheckPermissionAndGetRoute(start, status);
    }

    ThreadLocalUtil.routeInfo.set(ret);
    LOGGER.debug("src:{}, ret :{}",src, ret);

    return ret;
  }


  @Override
  public LocatedBlocks getBlockLocations(String src, long offset, long length) throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(src, OpType.READ);
    LocatedBlocks blocks = getClientProtocol(routeInfo, routeInfo.addr)
        .getBlockLocations(routeInfo.realPath, offset, length);
    return blocks;
  }


  @Override
  public FsServerDefaults getServerDefaults() throws IOException {
    String src = ContextUtil.getCurrentPath();
    VnnLogUtil.log("getServerDefaults == > current path " + src);
    if(Strings.isNullOrEmpty(src)){
      return getDefaults();
    }
    RouteInfo routeInfo = checkPermissionAndGetRoute(src, null);
    if (routeInfo == null || StringUtils.isEmpty(routeInfo.addr)) {
      throw new MigHadoopException(
          "can not get nn addr for getServerDefaults method tag: " + ContextUtil.getContextTag());
    }
    return getClientProtocol(routeInfo, routeInfo.addr).getServerDefaults();
  }

  private FsServerDefaults getDefaults() throws IOException {

    String checksumTypeStr = conf.get(DFS_CHECKSUM_TYPE_KEY, DFS_CHECKSUM_TYPE_DEFAULT);
    DataChecksum.Type checksumType;
    try {
      checksumType = DataChecksum.Type.valueOf(checksumTypeStr);
    } catch (IllegalArgumentException iae) {
      throw new IOException("Invalid checksum type in "
          + DFS_CHECKSUM_TYPE_KEY + ": " + checksumTypeStr);
    }

    return  new FsServerDefaults(
        conf.getLongBytes(DFS_BLOCK_SIZE_KEY, DFS_BLOCK_SIZE_DEFAULT),
        conf.getInt(DFS_BYTES_PER_CHECKSUM_KEY, DFS_BYTES_PER_CHECKSUM_DEFAULT),
        conf.getInt(DFS_CLIENT_WRITE_PACKET_SIZE_KEY, DFS_CLIENT_WRITE_PACKET_SIZE_DEFAULT),
        (short) conf.getInt(DFS_REPLICATION_KEY, DFS_REPLICATION_DEFAULT),
        conf.getInt(IO_FILE_BUFFER_SIZE_KEY, IO_FILE_BUFFER_SIZE_DEFAULT),
        conf.getBoolean(DFS_ENCRYPT_DATA_TRANSFER_KEY, DFS_ENCRYPT_DATA_TRANSFER_DEFAULT),
        conf.getLong(FS_TRASH_INTERVAL_KEY, FS_TRASH_INTERVAL_DEFAULT),
        checksumType,"");
  }

  @Override
  public HdfsFileStatus create(String src, FsPermission masked,
          String clientName, EnumSetWritable<CreateFlag> flag,
          boolean createParent, short replication, long blockSize,
          CryptoProtocolVersion[] supportedVersions, String ecPolicyName,
          String storagePolicy)
      throws IOException {

    RouteInfo routeInfo = null;
    HdfsFileStatus ret = null;
    routeInfo = checkPermissionAndGetRoute(src, OpType.WRITE);
    ret = getClientProtocol(routeInfo, routeInfo.addr).create(routeInfo.realPath, masked, clientName, flag,
        createParent, replication, blockSize, supportedVersions, ecPolicyName, storagePolicy);
    ClientNnCacheUtil.save(clientName,routeInfo.addr);

    return ret;
  }

  @Override
  public LastBlockWithStatus append(String src, String clientName, EnumSetWritable<CreateFlag> flag)
      throws IOException {

    RouteInfo routeInfo = checkPermissionAndGetRoute(src, OpType.WRITE);
    LastBlockWithStatus ret = getClientProtocol(routeInfo, routeInfo.addr).append(routeInfo.realPath, clientName, flag);
    ClientNnCacheUtil.save(clientName,routeInfo.addr);
    return ret;
  }

  @Override
  public boolean setReplication(String src, short replication)
      throws IOException {

    RouteInfo routeInfo = checkPermissionAndGetRoute(src, OpType.WRITE);

    return getClientProtocol(routeInfo, routeInfo.addr).setReplication(routeInfo.realPath, replication);
  }

  @Override
  public BlockStoragePolicy[] getStoragePolicies() throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(null, OpType.READ, true);
    return getClientProtocol(routeInfo, routeInfo.addr).getStoragePolicies();
  }

  @Override
  public void setStoragePolicy(String src, String policyName) throws IOException {

    RouteInfo routeInfo = checkPermissionAndGetRoute(src, OpType.WRITE, true);

    getClientProtocol(routeInfo, routeInfo.addr).setStoragePolicy(routeInfo.realPath, policyName);
  }

  @Override
  public void unsetStoragePolicy(String src) throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(src, OpType.WRITE, true);
    getClientProtocol(routeInfo, routeInfo.addr).unsetStoragePolicy(src);
  }

  @Override
  public BlockStoragePolicy getStoragePolicy(String path) throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(path, OpType.READ, true);
    return getClientProtocol(routeInfo, routeInfo.addr).getStoragePolicy(path);
  }

  @Override
  public void setPermission(String src, FsPermission permission) throws IOException {

    RouteInfo routeInfo = checkPermissionAndGetRoute(src, OpType.WRITE);

    getClientProtocol(routeInfo, routeInfo.addr).setPermission(routeInfo.realPath, permission);
  }

  @Override
  public void setOwner(String src, String username, String groupname) throws IOException {

    RouteInfo routeInfo = checkPermissionAndGetRoute(src, OpType.WRITE);

    getClientProtocol(routeInfo, routeInfo.addr).setOwner(routeInfo.realPath, username, groupname);
  }

  @Override
  public void abandonBlock(ExtendedBlock b, long fileId, String src, String holder)
      throws IOException {

    RouteInfo routeInfo = checkPermissionAndGetRoute(src, OpType.WRITE);

    getClientProtocol(routeInfo, routeInfo.addr).abandonBlock(b, fileId, routeInfo.realPath, holder);
  }

  @Override
  public LocatedBlock addBlock(String src, String clientName, ExtendedBlock previous,
      DatanodeInfo[] excludeNodes, long fileId, String[] favoredNodes,
      EnumSet<AddBlockFlag> addBlockFlags) throws IOException {

    //VnnLogUtil.log("addBlock " + src + " " + clientName);

    RouteInfo routeInfo = checkPermissionAndGetRoute(src, OpType.WRITE);

    LocatedBlock block = getClientProtocol(routeInfo, routeInfo.addr)
        .addBlock(routeInfo.realPath, clientName, previous, excludeNodes,
            fileId, favoredNodes, addBlockFlags);

    long blockSize = conf.getLong(DFS_BLOCK_SIZE_KEY, DFS_BLOCK_SIZE_DEFAULT);
    return block;
  }


  @Override
  public LocatedBlock getAdditionalDatanode(String src, long fileId, ExtendedBlock blk,
      DatanodeInfo[] existings, String[] existingStorageIDs,
      DatanodeInfo[] excludes, int numAdditionalNodes, String clientName) throws IOException {

    RouteInfo routeInfo = checkPermissionAndGetRoute(src, OpType.WRITE);

    return getClientProtocol(routeInfo, routeInfo.addr).getAdditionalDatanode(routeInfo.realPath, fileId,
        blk, existings, existingStorageIDs, excludes, numAdditionalNodes, clientName);
  }

  @Override
  public boolean complete(String src, String clientName, ExtendedBlock last, long fileId)
      throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(src, OpType.WRITE);
    boolean ret = getClientProtocol(routeInfo, routeInfo.addr).complete(routeInfo.realPath, clientName, last, fileId);
    if(ret){
      clientNNMap.remove(clientName);
    }
    return ret;
  }


  @Override
  public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
    //fixme
    RouteInfo routeInfo = checkPermissionAndGetRoute(null, OpType.WRITE);

    if (routeInfo == null || StringUtils.isEmpty(routeInfo.addr)) {
      throw new MigHadoopException("can not get nn addr for reportBadBlocks method tag: "
          + ContextUtil.getContextTag());
    }

    getClientProtocol(routeInfo, routeInfo.addr).reportBadBlocks(blocks);

  }

  @Override
  public boolean rename(String src, String dst) throws IOException {

    VnnLogUtil.log("rename " + src + " ==> " + dst);

    ThreadLocalUtil.dst.set(dst);

    RouteInfo routeInfo = checkPermissionAndGetRoute(src, OpType.WRITE);

    RouteInfo routeInfoDst = checkPermissionAndGetRoute(dst, OpType.WRITE);

    if (!routeInfo.addr.equals(routeInfoDst.addr)) {
      String msg = "rename error src:" + src + " and dst " + dst + " must in same real nn cluster!";
      VnnLogUtil.err(msg);
      throw new MigRouterException(msg);
    }

    String dest = routeInfoDst.realPath;

    //fixme
       /* if(VnnPathUtil.isMDFS(dst)&&VnnPathUtil.isTrashPath(dst)) {
            dest = VnnPathUtil.TRASH_PATH + routeInfoDst.realPath;
        }*/
    checkProtectDir(routeInfo.realPath);

    boolean ret = getClientProtocol(routeInfo, routeInfo.addr).rename(routeInfo.realPath, dest);

    if (VnnConfigUtil.isDebugOn()) {
      VnnLogUtil.log("rename from  " + src + " ==> realPath " + routeInfo.realPath +
          " final==>" + routeInfo.addr + dest + " ret " + ret);
    }

    return ret;
  }

  //todo must in same namenode
  @Override
  public void concat(String trg, String[] srcs)
      throws IOException{

    RouteInfo trgRouteInfo = checkPermissionAndGetRoute(trg, OpType.WRITE);
    RouteInfo[] routeInfos = new RouteInfo[srcs.length];

    for (int i = 0; i < srcs.length; i++) {
      routeInfos[i] = checkPermissionAndGetRoute(srcs[i], OpType.WRITE);

      if (!trgRouteInfo.addr.equals(routeInfos[i].addr)) {
        String msg =
            "concat error src:" + trg + " and dst " + srcs[i] + " must in same real nn cluster!";
        VnnLogUtil.err(msg);
        throw new MigRouterException(msg);
      }
      getClientProtocol(trgRouteInfo, trgRouteInfo.realPath).concat(trg, srcs);
    }
  }

  //fixme
  @Override
  public void rename2(String src, String dst, Options.Rename... options)
      throws IOException {
    VnnLogUtil.log("rename " + src + " ==> " + dst);
    ThreadLocalUtil.dst.set(dst);
    RouteInfo routeInfo = checkPermissionAndGetRoute(src, OpType.WRITE);
    String dest;
    RouteInfo routeInfoDst = checkPermissionAndGetRoute(dst, OpType.WRITE);
    if (!routeInfo.addr.equals(routeInfoDst.addr)) {
      String msg = "rename error src:" + src + " and dst " + dst + " must in same real nn cluster!";
      VnnLogUtil.err(msg);
      throw new MigRouterException(msg);
    }
    dest = routeInfoDst.realPath;
    if (VnnConfigUtil.isDebugOn()) {
      VnnLogUtil.debug("rename " + src + " ==> " + dest + " final==>" + routeInfo.addr + dest);
    }
    checkProtectDir(routeInfo.realPath);
    getClientProtocol(routeInfo, routeInfo.addr).rename2(routeInfo.realPath, dest, options);
  }

  @Override
  public boolean truncate(String src, long newLength, String clientName)
      throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(src, OpType.WRITE);
    return getClientProtocol(routeInfo, routeInfo.addr).truncate(routeInfo.realPath, newLength, clientName);
  }

  @Override
  public boolean delete(String src, boolean recursive)
      throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(src, OpType.WRITE);
    checkProtectDir(routeInfo.realPath);

    return getClientProtocol(routeInfo, routeInfo.addr).delete(routeInfo.realPath, recursive);
  }

  @Override
  public boolean mkdirs(String src, FsPermission masked, boolean createParent)
      throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(src, OpType.WRITE);
    return getClientProtocol(routeInfo, routeInfo.addr).mkdirs(routeInfo.realPath, masked, createParent);
  }

  @Override
  public DirectoryListing getListing(String src, byte[] startAfter, boolean needLocation)
      throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(src, OpType.READ);
    return getClientProtocol(routeInfo, routeInfo.addr)
        .getListing(routeInfo.realPath, startAfter, needLocation);
  }

  @Override
  public SnapshottableDirectoryStatus[] getSnapshottableDirListing() throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(null, OpType.READ);
    return getClientProtocol(routeInfo, routeInfo.addr).getSnapshottableDirListing();
  }

  @Override
  public SnapshotStatus[] getSnapshotListing(String snapshotRoot) throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(null, OpType.READ);
    return getClientProtocol(routeInfo, routeInfo.addr).getSnapshotListing(snapshotRoot);
  }

  @Override
  public void renewLease(String clientName) throws IOException {
    Set<String> nnSet = ClientNnCacheUtil.get(clientName);

    VnnLogUtil.debug("renewLease "+clientName+" nn num :"+nnSet.size()+" "+ClientNnCacheUtil.getLocalSize(clientName) + " " + ClientNnCacheUtil.getLocalSize());
    if (nnSet!= null && nnSet.size()>0) {
      for(String nn : nnSet){
        if( LocalCache.getClusterId(nn) != null ) {
          String activeNN = LocalCache.getNNAddr(LocalCache.getClusterId(nn));
          if(activeNN != null) {
            nn = activeNN;
          }
        }
        VnnLogUtil.log("renewLease "+clientName+" "+nn);
        if (!StringUtil.isEmpty(nn)) {
          getClientProtocol(null, nn).renewLease(clientName);
        }
      }
      return;
    }

    String files = ContextUtil.getCurrentWritingPath();
    if (!StringUtil.isEmpty(files)) {
      String[] srcs = files.split(",");
      VnnLogUtil.log("renewLease " + srcs.length);
      Set<String> nnList = new HashSet<>();
      for (String f : srcs) {
        try {
          RouteInfo routeInfo = checkPermissionAndGetRoute(f, OpType.WRITE);
          if (routeInfo != null && !StringUtil.isEmpty(routeInfo.addr)) {
            nnList.add(routeInfo.addr);
          } else {
            VnnLogUtil.warn(f + " can not find route :" + routeInfo);
          }
        } catch (Exception e) {
          e.printStackTrace();
          VnnLogUtil.err("add to nn set error " + clientName + " " + f + " " + e);
        }
      }

      for (String nn : nnList) {
        try {
          getClientProtocol(null, nn).renewLease(clientName);
        } catch (Exception e) {
          e.printStackTrace();
          VnnLogUtil.err("renewLease error " + clientName + " " + nn + " " + e);
        }
      }

    } else {
      //fixme
      RouteInfo routeInfo = checkPermissionAndGetRoute(null, null);
      if (routeInfo == null || StringUtils.isEmpty(routeInfo.addr)) {
        throw new MigHadoopException("can not get nn addr for renewLease method tag: "
            + ContextUtil.getContextTag());
      }
      getClientProtocol(routeInfo, routeInfo.addr).renewLease(clientName);
    }
  }


  @Override
  public boolean recoverLease(String src, String clientName) throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(src, OpType.WRITE);
    return getClientProtocol(routeInfo, routeInfo.addr).recoverLease(routeInfo.realPath, clientName);
  }

  @Override
  public long[] getStats() throws IOException {

    RouteInfo routeInfo = null;
    try {
      routeInfo = checkPermissionAndGetRoute(null, OpType.READ);
    } catch (Exception e) {
      e.printStackTrace();
      VnnLogUtil.err(ContextUtil.getClientInfo() + " getStats error " + e);
      VnnLogUtil.err(ContextUtil.getClientInfo() + " getStats error use default nn ");
      routeInfo = RouteUtil.getRouteInfoForMethodWithouPath();

    }

    if (routeInfo != null) {
      return getClientProtocol(routeInfo, routeInfo.addr).getStats();
    }
    throw new MigNotSupportProxyMethodException("getStats");
  }

  @Override
  public ReplicatedBlockStats getReplicatedBlockStats() throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(null, OpType.READ, true);
    return getClientProtocol(routeInfo, routeInfo.addr).getReplicatedBlockStats();
  }

  @Override
  public ECBlockGroupStats getECBlockGroupStats() throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(null, OpType.READ, true);
    return getClientProtocol(routeInfo, routeInfo.addr).getECBlockGroupStats();
  }

  @Override
  public DatanodeInfo[] getDatanodeReport(HdfsConstants.DatanodeReportType type)
      throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(null, OpType.READ, true);
    return getClientProtocol(routeInfo, routeInfo.addr).getDatanodeReport(type);
  }

  @Override
  public DatanodeStorageReport[] getDatanodeStorageReport(HdfsConstants.DatanodeReportType type)
      throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(null, OpType.READ, true);
    return getClientProtocol(routeInfo, routeInfo.addr).getDatanodeStorageReport(type);
  }

  @Override
  public long getPreferredBlockSize(String filename) throws IOException, UnresolvedLinkException {

    RouteInfo routeInfo = checkPermissionAndGetRoute(filename, OpType.READ);

    return getClientProtocol(routeInfo, routeInfo.addr).getPreferredBlockSize(routeInfo.realPath);
  }

  @Override
  public boolean setSafeMode(HdfsConstants.SafeModeAction action, boolean isChecked)
      throws IOException {

    String src = ContextUtil.getCurrentPath();

    VnnLogUtil.log("getServerDefaults == > current path " + src);

    RouteInfo routeInfo = checkPermissionAndGetRoute(src, null);

    if (routeInfo == null || StringUtils.isEmpty(routeInfo.addr)) {
      throw new MigHadoopException(
          "can not get nn addr for getServerDefaults method tag: " + ContextUtil.getContextTag());
    }

    return getClientProtocol(routeInfo, routeInfo.addr).setSafeMode(action, isChecked);
  }

  @Override
  public boolean saveNamespace(long timeWindow, long txGap) throws IOException {
    return false;
  }

  @Override
  public long rollEdits() throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(null, OpType.READ);
    return getClientProtocol(routeInfo, routeInfo.addr).rollEdits();
  }

  @Override
  public boolean restoreFailedStorage(String arg) throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(null, OpType.READ);
    return getClientProtocol(routeInfo, routeInfo.addr).restoreFailedStorage(arg);
  }

  @Override
  public void refreshNodes() throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(null, OpType.READ, true);
    getClientProtocol(routeInfo, routeInfo.addr).refreshNodes();
  }

  @Override
  public void finalizeUpgrade() throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(null, OpType.READ);
    getClientProtocol(routeInfo, routeInfo.addr).finalizeUpgrade();
  }

  @Override
  public boolean upgradeStatus() throws IOException {
    return false;
  }

  @Override
  public RollingUpgradeInfo rollingUpgrade(HdfsConstants.RollingUpgradeAction action)
      throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(null, OpType.READ);
    return getClientProtocol(routeInfo, routeInfo.addr).rollingUpgrade(action);
  }

  @Override
  public CorruptFileBlocks listCorruptFileBlocks(String path, String cookie) throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(null, OpType.READ);
    return getClientProtocol(routeInfo, routeInfo.addr).listCorruptFileBlocks(path, cookie);
  }

  @Override
  public void metaSave(String filename) throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(null, OpType.READ);
    getClientProtocol(routeInfo, routeInfo.addr).metaSave(filename);
  }

  @Override
  public void setBalancerBandwidth(long bandwidth) throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(null, OpType.READ, true);
    getClientProtocol(routeInfo, routeInfo.addr).setBalancerBandwidth(bandwidth);
  }

  @Override
  public HdfsFileStatus getFileInfo(String src)
      throws IOException {

    RouteInfo routeInfo = checkPermissionAndGetRoute(src, OpType.READ);

    return getClientProtocol(routeInfo, routeInfo.addr).getFileInfo(routeInfo.realPath);
  }

  @Override
  public boolean isFileClosed(String src)
      throws IOException {

    RouteInfo routeInfo = checkPermissionAndGetRoute(src, OpType.READ);

    return getClientProtocol(routeInfo, routeInfo.addr).isFileClosed(routeInfo.realPath);
  }

  @Override
  public HdfsFileStatus getFileLinkInfo(String src)
      throws IOException {

    RouteInfo routeInfo = checkPermissionAndGetRoute(src, OpType.READ);

    return getClientProtocol(routeInfo, routeInfo.addr).getFileLinkInfo(routeInfo.realPath);
  }

  @Override
  public HdfsLocatedFileStatus getLocatedFileInfo(String src, boolean needBlockToken) throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(src, OpType.READ);
    return getClientProtocol(routeInfo, routeInfo.addr).getLocatedFileInfo(src, needBlockToken);
  }

  @Override
  public ContentSummary getContentSummary(String path)
      throws IOException {

    RouteInfo routeInfo = checkPermissionAndGetRoute(path, OpType.READ);

    return getClientProtocol(routeInfo, routeInfo.addr).getContentSummary(routeInfo.realPath);
  }

  @Override
  public void setQuota(String path, long namespaceQuota, long storagespaceQuota, StorageType type)
      throws IOException {

    RouteInfo routeInfo = checkPermissionAndGetRoute(path, OpType.WRITE);

    getClientProtocol(routeInfo, routeInfo.addr).setQuota(routeInfo.realPath,
        namespaceQuota, storagespaceQuota, type);
  }

  @Override
  public void fsync(String src, long inodeId, String client, long lastBlockLength)
      throws IOException {

    RouteInfo routeInfo = checkPermissionAndGetRoute(src, OpType.WRITE);

    getClientProtocol(routeInfo, routeInfo.addr).fsync(routeInfo.realPath,
        inodeId, client, lastBlockLength);
  }

  @Override
  public void setTimes(String src, long mtime, long atime)
      throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(src, OpType.WRITE);

    getClientProtocol(routeInfo, routeInfo.addr).setTimes(routeInfo.realPath, mtime, atime);
  }

  @Override
  public void createSymlink(String target, String link, FsPermission dirPerm, boolean createParent)
      throws IOException {

    RouteInfo routeInfo = checkPermissionAndGetRoute(target, OpType.WRITE);

    getClientProtocol(routeInfo, routeInfo.addr)
        .createSymlink(routeInfo.realPath, link, dirPerm, createParent);
  }

  @Override
  public String getLinkTarget(String path)
      throws IOException {

    RouteInfo routeInfo = checkPermissionAndGetRoute(path, OpType.READ);

    return getClientProtocol(routeInfo, routeInfo.addr).getLinkTarget(routeInfo.realPath);
  }

  @Override
  public LocatedBlock updateBlockForPipeline(ExtendedBlock block, String clientName)
      throws IOException {
    Set<String> nnSet = ClientNnCacheUtil.get(clientName);
    VnnLogUtil.debug("updateBlockForPipeline "+clientName+" nn num :"+nnSet.size()+" "+ClientNnCacheUtil.getLocalSize(clientName) + " " + ClientNnCacheUtil.getLocalSize());
    if (nnSet!= null && nnSet.size()>0) {
      for(String nn : nnSet){
        if( LocalCache.getClusterId(nn) != null ) {
          String activeNN = LocalCache.getNNAddr(LocalCache.getClusterId(nn));
          if(activeNN != null) {
            nn = activeNN;
          }
        }
        VnnLogUtil.log("updateBlockForPipeline "+clientName+" "+nn);
        if (!StringUtil.isEmpty(nn)) {
          return getClientProtocol(null, nn).updateBlockForPipeline(block, clientName);
        }
      }
    }
    String src = ContextUtil.getCurrentPath();

    if (VnnConfigUtil.isDebugOn()) {
      VnnLogUtil.log("updateBlockForPipeline == >getCurrentPath ==> " + src);
    }

    RouteInfo routeInfo = checkPermissionAndGetRoute(src, OpType.WRITE);

    return getClientProtocol(routeInfo, routeInfo.addr).updateBlockForPipeline(block, clientName);
  }

  //added by chunxiao
  public LocatedBlock updateBlockForPipeline(String src, ExtendedBlock block, String clientName)
      throws IOException {

    RouteInfo routeInfo = checkPermissionAndGetRoute(src, OpType.WRITE);

    return getClientProtocol(routeInfo, routeInfo.addr).updateBlockForPipeline(block, clientName);
  }

  @Override
  public void updatePipeline(String clientName, ExtendedBlock oldBlock,
      ExtendedBlock newBlock,
      DatanodeID[] newNodes, String[] newStorageIDs) throws IOException {
    Set<String> nnSet = ClientNnCacheUtil.get(clientName);
    VnnLogUtil.debug("updatePipeline "+clientName+" nn num :"+nnSet.size()+" "+ClientNnCacheUtil.getLocalSize(clientName) + " " + ClientNnCacheUtil.getLocalSize());
    if (nnSet!= null && nnSet.size()>0) {
      for(String nn : nnSet){
        if( LocalCache.getClusterId(nn) != null ) {
          String activeNN = LocalCache.getNNAddr(LocalCache.getClusterId(nn));
          if(activeNN != null) {
            nn = activeNN;
          }
        }
        VnnLogUtil.log("updatePipeline "+clientName+" "+nn);
        if (!StringUtil.isEmpty(nn)) {
          getClientProtocol(null, nn)
              .updatePipeline(clientName, oldBlock, newBlock, newNodes, newStorageIDs);
        }
      }
      return;
    }
    String src = ContextUtil.getCurrentPath();
    if (VnnConfigUtil.isDebugOn()) {
      VnnLogUtil.log("updatePipeline == >getCurrentPath ==> " + src);
    }
    RouteInfo routeInfo = checkPermissionAndGetRoute(src, OpType.WRITE);
    getClientProtocol(routeInfo, routeInfo.addr).updatePipeline(clientName, oldBlock,
        newBlock, newNodes, newStorageIDs);

  }

  //added by chunxiao
  public void updatePipeline(String src, String clientName, ExtendedBlock oldBlock,
      ExtendedBlock newBlock,
      DatanodeID[] newNodes, String[] newStorageIDs) throws IOException {

    RouteInfo routeInfo = checkPermissionAndGetRoute(src, OpType.WRITE);

    getClientProtocol(routeInfo, routeInfo.addr).updatePipeline(clientName, oldBlock,
        newBlock, newNodes, newStorageIDs);

  }

  @Override
  public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer) throws IOException {
    return securityManager.getDelegationToken(renewer);
  }

  @Override
  public long renewDelegationToken(Token<DelegationTokenIdentifier> token) throws IOException {
    return securityManager.renewDelegationToken(token);
  }

  @Override
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token) throws IOException {
    securityManager.cancelDelegationToken(token);
  }

  @Override
  public DataEncryptionKey getDataEncryptionKey() throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(null, OpType.READ);
    return getClientProtocol(routeInfo, routeInfo.addr).getDataEncryptionKey();
  }

  @Override
  public String createSnapshot(String snapshotRoot, String snapshotName) throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(null, OpType.READ);
    return getClientProtocol(routeInfo, routeInfo.addr).createSnapshot(snapshotRoot, snapshotName);
  }

  @Override
  public void deleteSnapshot(String snapshotRoot, String snapshotName) throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(null, OpType.READ);
    getClientProtocol(routeInfo, routeInfo.addr).deleteSnapshot(snapshotRoot, snapshotName);
  }

  @Override
  public void renameSnapshot(String snapshotRoot, String snapshotOldName, String snapshotNewName)
      throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(null, OpType.READ);
    getClientProtocol(routeInfo, routeInfo.addr)
        .renameSnapshot(snapshotRoot, snapshotOldName, snapshotNewName);
  }

  @Override
  public void allowSnapshot(String snapshotRoot) throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(null, OpType.READ);
    getClientProtocol(routeInfo, routeInfo.addr).allowSnapshot(snapshotRoot);
  }

  @Override
  public void disallowSnapshot(String snapshotRoot) throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(null, OpType.READ);
    getClientProtocol(routeInfo, routeInfo.addr).disallowSnapshot(snapshotRoot);
  }

  @Override
  public SnapshotDiffReport getSnapshotDiffReport(String snapshotRoot, String fromSnapshot,
      String toSnapshot) throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(null, OpType.READ);
    return getClientProtocol(routeInfo, routeInfo.addr)
        .getSnapshotDiffReport(snapshotRoot, fromSnapshot, toSnapshot);
  }

  @Override
  public SnapshotDiffReportListing getSnapshotDiffReportListing(String snapshotRoot, String fromSnapshot,
          String toSnapshot, byte[] startPath, int index) throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(null, OpType.READ);
    return getClientProtocol(routeInfo, routeInfo.addr)
            .getSnapshotDiffReportListing(snapshotRoot, fromSnapshot, toSnapshot, startPath, index);
  }

  @Override
  public long addCacheDirective(CacheDirectiveInfo directive, EnumSet<CacheFlag> flags)
      throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(null, OpType.READ);
    return getClientProtocol(routeInfo, routeInfo.addr).addCacheDirective(directive, flags);
  }

  @Override
  public void modifyCacheDirective(CacheDirectiveInfo directive, EnumSet<CacheFlag> flags)
      throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(null, OpType.READ);
    getClientProtocol(routeInfo, routeInfo.addr).modifyCacheDirective(directive, flags);
  }

  @Override
  public void removeCacheDirective(long id) throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(null, OpType.READ);
    getClientProtocol(routeInfo, routeInfo.addr).removeCacheDirective(id);
  }

  @Override
  public BatchedRemoteIterator.BatchedEntries<CacheDirectiveEntry> listCacheDirectives(long prevId,
      CacheDirectiveInfo filter) throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(null, OpType.READ);
    return getClientProtocol(routeInfo, routeInfo.addr).listCacheDirectives(prevId, filter);
  }

  @Override
  public void addCachePool(CachePoolInfo info) throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(null, OpType.READ);
    getClientProtocol(routeInfo, routeInfo.addr).addCachePool(info);
  }

  @Override
  public void modifyCachePool(CachePoolInfo req) throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(null, OpType.READ);
    getClientProtocol(routeInfo, routeInfo.addr).modifyCachePool(req);
  }

  @Override
  public void removeCachePool(String pool) throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(null, OpType.READ);
    getClientProtocol(routeInfo, routeInfo.addr).removeCachePool(pool);
  }

  @Override
  public BatchedRemoteIterator.BatchedEntries<CachePoolEntry> listCachePools(String prevPool)
      throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(null, OpType.READ);
    return getClientProtocol(routeInfo, routeInfo.addr).listCachePools(prevPool);
  }

  @Override
  public void modifyAclEntries(String src, List<AclEntry> aclSpec) throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(src, OpType.WRITE);
    getClientProtocol(routeInfo, routeInfo.addr).modifyAclEntries(src, aclSpec);
  }

  @Override
  public void removeAclEntries(String src, List<AclEntry> aclSpec) throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(src, OpType.READ);
    getClientProtocol(routeInfo, routeInfo.addr).removeAclEntries(src, aclSpec);
  }

  @Override
  public void removeDefaultAcl(String src) throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(src, OpType.READ);
    getClientProtocol(routeInfo, routeInfo.addr).removeDefaultAcl(src);
  }

  @Override
  public void removeAcl(String src) throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(src, OpType.READ);
    getClientProtocol(routeInfo, routeInfo.addr).removeAcl(src);
  }

  @Override
  public void setAcl(String src, List<AclEntry> aclSpec) throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(src, OpType.READ);
    getClientProtocol(routeInfo, routeInfo.addr).setAcl(src, aclSpec);
  }

  @Override
  public AclStatus getAclStatus(String src) throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(src, OpType.READ);
    return getClientProtocol(routeInfo, routeInfo.addr).getAclStatus(src);
  }

  @Override
  public void createEncryptionZone(String src, String keyName) throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(src, OpType.READ);
    getClientProtocol(routeInfo, routeInfo.addr).createEncryptionZone(src, keyName);
  }

  @Override
  public EncryptionZone getEZForPath(String src) throws IOException {

    VnnLogUtil.log("getEZForPath===" + src);

    RouteInfo routeInfo = checkPermissionAndGetRoute(src, OpType.READ);

    EncryptionZone ret = null;
    try {
      ret = getClientProtocol(routeInfo, routeInfo.addr).getEZForPath(routeInfo.realPath);

    } catch (RemoteException e) {

      Throwable t = e.unwrapRemoteException();

      if (t instanceof RpcNoSuchMethodException) {
        VnnLogUtil.err(
            "getEZForPath=====RpcNoSuchMethodException " + src + " " + ContextUtil.getClientInfo()
                + " " + e);
      } else {
        throw e;
      }
    }
    VnnLogUtil.log("getEZForPath===ret === " + ret);

    return ret;

  }

  @Override
  public BatchedRemoteIterator.BatchedEntries<EncryptionZone> listEncryptionZones(long prevId)
      throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(null, OpType.READ);
    return getClientProtocol(routeInfo, routeInfo.addr).listEncryptionZones(prevId);
  }

  @Override
  public void reencryptEncryptionZone(String zone, ReencryptAction action) throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(null, OpType.READ);
    getClientProtocol(routeInfo, routeInfo.addr).reencryptEncryptionZone(zone, action);
  }

  @Override
  public BatchedEntries<ZoneReencryptionStatus> listReencryptionStatus(long prevId) throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(null, OpType.READ);
    return getClientProtocol(routeInfo, routeInfo.addr).listReencryptionStatus(prevId);
  }

  @Override
  public void setXAttr(String src, XAttr xAttr, EnumSet<XAttrSetFlag> flag) throws IOException {

    RouteInfo routeInfo = checkPermissionAndGetRoute(src, OpType.WRITE);

    getClientProtocol(routeInfo, routeInfo.addr).setXAttr(routeInfo.realPath, xAttr, flag);
  }

  @Override
  public List<XAttr> getXAttrs(String src, List<XAttr> xAttrs) throws IOException {

    RouteInfo routeInfo = checkPermissionAndGetRoute(src, OpType.READ);

    return getClientProtocol(routeInfo, routeInfo.addr).getXAttrs(routeInfo.realPath, xAttrs);
  }

  @Override
  public List<XAttr> listXAttrs(String src) throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(src, OpType.READ);

    return getClientProtocol(routeInfo, routeInfo.addr).listXAttrs(routeInfo.realPath);
  }

  @Override
  public void removeXAttr(String src, XAttr xAttr) throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(src, OpType.WRITE);

    getClientProtocol(routeInfo, routeInfo.addr).removeXAttr(routeInfo.realPath, xAttr);
  }

  @Override
  public void checkAccess(String path, FsAction mode) throws IOException {

    RouteInfo routeInfo = checkPermissionAndGetRoute(path, OpType.READ);

    getClientProtocol(routeInfo, routeInfo.addr).checkAccess(routeInfo.realPath, mode);
  }

  @Override
  public long getCurrentEditLogTxid() throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(null, OpType.READ);
    return getClientProtocol(routeInfo, routeInfo.addr).getCurrentEditLogTxid();
  }

  @Override
  public EventBatchList getEditsFromTxid(long txid) throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(null, OpType.READ);
    return getClientProtocol(routeInfo, routeInfo.addr).getEditsFromTxid(txid);
  }

  @Override
  public QuotaUsage getQuotaUsage(String path) throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(null, OpType.READ);
    return getClientProtocol(routeInfo, routeInfo.addr).getQuotaUsage(path);
  }

  @Override
  public BatchedEntries<OpenFileEntry> listOpenFiles(long prevId) throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(null, OpType.READ);
    return getClientProtocol(routeInfo, routeInfo.addr).listOpenFiles(prevId);
  }

  @Override
  public BatchedEntries<OpenFileEntry> listOpenFiles(long prevId, EnumSet<OpenFilesType> openFilesTypes, String path)
          throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(null, OpType.READ);
    return getClientProtocol(routeInfo, routeInfo.addr).listOpenFiles(prevId, openFilesTypes, path);
  }

  @Override
  public void refreshProtection() throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(null, OpType.READ);
    getClientProtocol(routeInfo, routeInfo.addr).refreshProtection();
  }

  @Override
  public String getProtection() throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(null, OpType.READ);
    return getClientProtocol(routeInfo, routeInfo.addr).getProtection();
  }

  @Override
  public String getRemotePath(String src) throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(null, OpType.READ);
    return getClientProtocol(routeInfo, routeInfo.addr).getRemotePath(src);
  }

  private void checkProtectDir(final String dir) {
    if (VnnConfigUtil.isProtectDirOn()) {
      boolean isProtectDir = VnnPathUtil.isProtectDir(dir);
      if (isProtectDir) {

        VnnLogUtil.err("can not delete protect dir of " + dir);

        throw new MigPermissionDenyException("can not delete protect dir of " + dir);
      }
    }
  }

  @Override
  public AddErasureCodingPolicyResponse[] addErasureCodingPolicies(
      ErasureCodingPolicy[] policies) throws IOException {
    throw new UnsupportedActionException("MDFS NameNode "
        + "does not support EC.");
  }

  @Override
  public void removeErasureCodingPolicy(String ecPolicyName) throws IOException {
    throw new UnsupportedActionException("MDFS NameNode "
            + "does not support EC.");
  }

  @Override
  public void enableErasureCodingPolicy(String ecPolicyName)
      throws IOException {
    throw new UnsupportedActionException("MDFS NameNode "
        + "does not support EC.");
  }

  @Override
  public void disableErasureCodingPolicy(String ecPolicyName) throws IOException {
    throw new UnsupportedActionException("MDFS NameNode "
            + "does not support EC.");
  }

  @Override
  public ErasureCodingPolicyInfo[] getErasureCodingPolicies() throws IOException {
    throw new UnsupportedActionException("MDFS NameNode "
            + "does not support EC.");
  }

  @Override
  public Map<String, String> getErasureCodingCodecs() throws IOException {
    throw new UnsupportedActionException("MDFS NameNode "
            + "does not support EC.");
  }

  @Override
  public ErasureCodingPolicy getErasureCodingPolicy(String src)
      throws IOException {
    throw new UnsupportedActionException("MDFS NameNode "
        + "does not support EC.");
  }

  @Override
  public void unsetErasureCodingPolicy(String src) throws IOException {
    throw new UnsupportedActionException("MDFS NameNode "
            + "does not support EC.");
  }

  @Override
  public void setErasureCodingPolicy(String src, String ecPolicyName)
      throws IOException {
    throw new UnsupportedActionException("MDFS NameNode "
        + "does not support EC.");
  }

  @Override
  public void msync() throws IOException {
    throw new UnsupportedActionException("MDFS NameNode "
        + "does not support msync.");
  }

  @Override
  public void satisfyStoragePolicy(String path) throws IOException {
    RouteInfo routeInfo = checkPermissionAndGetRoute(null, OpType.READ);
    getClientProtocol(routeInfo, routeInfo.addr).satisfyStoragePolicy(path);
  }

  @Override
  public HAServiceState getHAServiceState() throws IOException {
    throw new UnsupportedActionException("MDFS NameNode "
        + "does not support getHAServiceState.");
  }
}
