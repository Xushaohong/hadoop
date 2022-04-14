package com.tencent.mdfs.nnproxy.impl;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_TIMEOUT_KEY;

import com.google.protobuf.BlockingService;

import com.tencent.mdfs.config.RBFConfigKeys;
import com.tencent.mdfs.nnproxy.VnnHandler;
import com.tencent.mdfs.nnproxy.conn.ConnectionManager;
import com.tencent.mdfs.nnproxy.security.RouterSecurityManager;
import com.tencent.mdfs.util.MonitorUtils;
import com.tencent.mdfs.util.VnnConfigUtil;
import com.tencent.mdfs.util.VnnLogUtil;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos;
import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolServerSideTranslatorPB;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;


public class CloudHdfsServer {

  private RPC.Server clientRpcServer;
  private final ConnectionManager connectionManager;

  private static final Logger LOGGER = LogManager.getLogger(CloudHdfsServer.class);
  public final RouterSecurityManager securityManager;

  public CloudHdfsServer(final Configuration conf) throws IOException {
    LOGGER.info("CloudHdfsServer init ");
    this.securityManager = new RouterSecurityManager(conf);
    RPC.setProtocolEngine(conf, ClientNamenodeProtocolPB.class,
        ProtobufRpcEngine.class);
    Configuration clientConf = getClientConfiguration(conf);
    this.connectionManager = new ConnectionManager(clientConf);
    this.connectionManager.start();

    Thread connectionManagerMonitor = new Thread() {
      @Override
      public void run() {
        long x = 1;
        while (true) {
          try {
            int numActiveConnections = connectionManager.getNumActiveConnections();
            int numConnectionPools = connectionManager.getNumConnectionPools();
            int numConnections = connectionManager.getNumConnections();
            int numCreatingConnections = connectionManager.getNumCreatingConnections();

            VnnLogUtil.log("ConnectionManager-Info-nums: numActiveConnections="
                    +numActiveConnections+", numConnectionPools="
                    +numConnectionPools+", numConnections="
                    +numConnections+", numCreatingConnections="+numCreatingConnections);
            if ( x % 60 == 0) {
              VnnLogUtil.log("ConnectionManager-Info-JSON: "+connectionManager.getJSON());
            }

            MonitorUtils.reportConnectionManagerPools(numActiveConnections, numConnectionPools, numConnections, numCreatingConnections);
            Thread.sleep(1000L);
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      }
    };
    connectionManagerMonitor.setDaemon(true);
    connectionManagerMonitor.start();

    ClientProtocol serverSideClientProtocol = new MigServerSideClientProtocolImpl(conf, securityManager, connectionManager);
    VnnHandler vnnHandler = new VnnHandler(serverSideClientProtocol);
    ClientProtocol clientProtocolProxy = (ClientProtocol)Proxy.newProxyInstance(
        ClientProtocol.class.getClassLoader(),
        serverSideClientProtocol.getClass().getInterfaces(),vnnHandler);

    ClientNamenodeProtocolServerSideTranslatorPB
        clientProtocolServerTranslator = new ClientNamenodeProtocolServerSideTranslatorPB(
        clientProtocolProxy);

    BlockingService clientNNPbService = ClientNamenodeProtocolProtos.ClientNamenodeProtocol.
        newReflectiveBlockingService(clientProtocolServerTranslator);





    this.clientRpcServer = new RPC.Builder(conf)
        .setProtocol(
            ClientNamenodeProtocolPB.class)
        .setInstance(clientNNPbService)
        .setBindAddress(VnnConfigUtil.getNameNodeServerAddr())
        .setPort(VnnConfigUtil.getNameNodeServerPort())
        .setNumHandlers(VnnConfigUtil.getHandlerNum())
        .setVerbose(false)
        .setSecretManager(securityManager.getSecretManager())
        .build();
    LOGGER.info("CloudHdfsServer init ok ");
  }

  /**
   * Get the configuration for the RPC client. It takes the Router
   * configuration and transforms it into regular RPC Client configuration.
   * @param conf Input configuration.
   * @return Configuration for the RPC client.
   */
  private Configuration getClientConfiguration(final Configuration conf) {
    Configuration clientConf = new Configuration(conf);
    int maxRetries = conf.getInt(
            RBFConfigKeys.DFS_ROUTER_CLIENT_MAX_RETRIES_TIME_OUT,
            RBFConfigKeys.DFS_ROUTER_CLIENT_MAX_RETRIES_TIME_OUT_DEFAULT);
    if (maxRetries >= 0) {
      clientConf.setInt(
              IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY, maxRetries);
    }
    long connectTimeOut = conf.getTimeDuration(
            RBFConfigKeys.DFS_ROUTER_CLIENT_CONNECT_TIMEOUT,
            RBFConfigKeys.DFS_ROUTER_CLIENT_CONNECT_TIMEOUT_DEFAULT,
            TimeUnit.MILLISECONDS);
    if (connectTimeOut >= 0) {
      clientConf.setLong(IPC_CLIENT_CONNECT_TIMEOUT_KEY, connectTimeOut);
    }
    return clientConf;
  }

  public void start() {
    try {
      this.securityManager.start();
      this.clientRpcServer.start();
      System.out.println("CloudHdfsServer started");
      LOGGER.info(" ====== CloudHdfsServer start done =====");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
