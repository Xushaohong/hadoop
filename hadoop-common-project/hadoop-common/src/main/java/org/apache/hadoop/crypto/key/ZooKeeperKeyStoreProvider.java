/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.crypto.key;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URI;
import java.security.Key;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.ZKUtil.ZKAuthInfo;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;

/**
 * KeyProvider based on Java's KeyStore file format. But file is stored in a
 * ZooKeeper cluster, cut into multiple ZNode, each ZNode stores a part of the
 * file.
 *
 * File size can be saved per znode can be configured with the option
 * HADOOP_ZOOKEEPER_KEYSTORE_FILE_SIZE_PER_ZNODE, The maximum value of this
 * option is 1M and default size is 512K.
 *
 * This provider use the following name mangling:
 *  zks://zkHost1:2181,zkHost2:2181,zkHost3:2181
 * <p/>
 * If the <code>HADOOP_KEYSTORE_PASSWORD</code> environment variable is set,
 * its value is used as the password for the keystore.
 * <p/>
 * If the <code>HADOOP_KEYSTORE_PASSWORD</code> environment variable is not set,
 * the password for the keystore is read from file specified in the
 * {@link #KEYSTORE_PASSWORD_FILE_KEY} configuration property. The password file
 * is looked up in Hadoop's configuration directory via the classpath.
 * <p/>
 * <p/>
 * If the environment variable, nor the property are not set, the password used
 * is 'none'.
 * <p/>
 * It is expected for encrypted InputFormats and OutputFormats to copy the keys
 * from the original provider into the job's Credentials object, which is
 * accessed via the UserProvider. Therefore, this provider won't be used by
 * MapReduce tasks.
 */
@InterfaceAudience.Private
public final class ZooKeeperKeyStoreProvider extends KeyProvider {
  private static final String KEY_METADATA = "KeyMetadata";
  private static final Logger LOG =
      LoggerFactory.getLogger(ZooKeeperKeyStoreProvider.class);
  /*
   * The external schema is zks, and internally use jceks to accomplish file
   * encryption, decryption, integrity check, access password control, etc.
   */
  public static final String SCHEME_NAME = "zks";
  private static final String INTERNAL_SCHEME_NAME = "jceks";
  private static final String ZK_KEY_ZNODE_NAME_PREFIX = "part-";

  public static final String KEYSTORE_PASSWORD_FILE_KEY =
      "hadoop.security.keystore.zookeeper-keystore-provider.password-file";
  public static final char[] KEYSTORE_PASSWORD_DEFAULT = "none".toCharArray();

  // local key file
  public static final String KEYSTORE_LOCAL_FILE_KEY =
      "hadoop.security.keystore.zookeeper-keystore-provider.local.key-file";
  public static final String KEYSTORE_LOCAL_FILE_DEFAULT =
      "/tmp/ZookeeperKeyStore.keyFile";

  // timeout of zookeeper session timeout in ms
  private static final String ZK_SESSION_TIMEOUT_KEY =
      "hadoop.security.keystore.zookeeper-keystore-provider.session-timeout.ms";
  private static final int ZK_SESSION_TIMEOUT_DEFAULT = 5*1000;

  // number of zookeeper operation retry times
  public static final String ZK_OP_RETRIES_KEY =
      "hadoop.security.keystore.zookeeper-keystore-provider.zk.op.retries";
  public static final int ZK_OP_RETRIES_DEFAULT = 3;

  // max data size of each znode
  public static final String ZK_ZNODE_DATA_SIZE_KEY =
      "hadoop.security.keystore.zookeeper-keystore-provider.zk.znode.datasize";
  public static final int ZK_ZNODE_DATA_SIZE_DEFAULT = 512*1024;

  // acl of znode
  public static final String ZK_ACL_KEY =
      "hadoop.security.keystore.zookeeper-keystore-provider.zk.acl";
  private static final String ZK_ACL_DEFAULT = "world:anyone:rwcda";

  // auth info for zk
  public static final String ZK_AUTH_KEY =
      "hadoop.security.keystore.zookeeper-keystore-provider.zk.auth";

  private final URI providerUri;

  private String zkHostPort;
  private int zkSessionTimeout;
  private List<ACL> zkAcl;
  private List<ZKAuthInfo> zkAuthInfo;
  private String znodeWorkingDir;
  private int maxRetryNum;
  private int znodeDataSize;
  private ZooKeeper zkClient;
  private Lock zkClientRLock;
  private Lock zkClientWLock;
  private WatcherWithClientRef watcher;

  private KeyStore keyStore;
  private char[] password;
  private boolean changed = false;
  private Lock readLock;
  private Lock writeLock;

  private File localKeyFile;
  private File localKeyFilePrev;

  private final Map<String, Metadata> cache = new HashMap<String, Metadata>();

  private ZooKeeperKeyStoreProvider(URI uri, Configuration conf)
      throws IOException {
    this(uri, conf, true);
  }

  private ZooKeeperKeyStoreProvider(URI uri, Configuration conf,
      boolean loadKeys) throws IOException {
    super(conf);
    this.providerUri = uri;

    // Init ZooKeeper client
    try {
      initZK();
    } catch (IOException e) {
      String errMsg = "Unable to init ZooKeeperKeyProvider. Unable "
          + "to connect to ZooKeeper quorum at " + providerUri.getAuthority()
          + ". Please check the configured value and ensure that ZooKeeper "
          + "is running.";
      LOG.error(errMsg + "\n" + getErrorMessage(e));
      throw new IOException(errMsg, e);
    }

    // Init backing keyStore
    try {
      initKeyStore(loadKeys);
    } catch (IOException e) {
      String errMsg = "Unable to init keyStore.";
      LOG.error(errMsg + "\n" + getErrorMessage(e));
      throw new IOException(errMsg, e);
    }
  }

  private void initZK() throws IOException {
    Configuration conf = getConf();

    zkHostPort = providerUri.getAuthority();
    znodeWorkingDir = getParentZnode(providerUri.getPath());
    zkSessionTimeout = conf.getInt(ZK_SESSION_TIMEOUT_KEY,
        ZK_SESSION_TIMEOUT_DEFAULT);
    maxRetryNum = conf.getInt(ZK_OP_RETRIES_KEY, ZK_OP_RETRIES_DEFAULT);
    znodeDataSize = conf.getInt(ZK_ZNODE_DATA_SIZE_KEY,
        ZK_ZNODE_DATA_SIZE_DEFAULT);

    // Parse ACLs from configuration.
    String zkAclConf = conf.get(ZK_ACL_KEY, ZK_ACL_DEFAULT);
    zkAclConf = org.apache.hadoop.util.ZKUtil.resolveConfIndirection(zkAclConf);
    zkAcl = org.apache.hadoop.util.ZKUtil.parseACLs(zkAclConf);
    if (zkAcl.isEmpty()) {
      zkAcl = Ids.CREATOR_ALL_ACL;
    }

    // Parse authentication from configuration.
    String zkAuthConf = conf.get(ZK_AUTH_KEY);
    zkAuthConf = org.apache.hadoop.util.ZKUtil
        .resolveConfIndirection(zkAuthConf);
    if (zkAuthConf != null) {
      zkAuthInfo = org.apache.hadoop.util.ZKUtil.parseAuth(zkAuthConf);
    } else {
      zkAuthInfo = Collections.emptyList();
    }

    ReadWriteLock zkClientlock = new ReentrantReadWriteLock(true);
    zkClientRLock= zkClientlock.readLock();
    zkClientWLock = zkClientlock.writeLock();

    // createConnection for future API calls
    createConnection();
  }

  private void initKeyStore(boolean loadKeys) throws IOException{
    // Get the key storage access password from the conf
    String pwFile = getConf().get(KEYSTORE_PASSWORD_FILE_KEY);
    if (pwFile != null) {
      password = Files.asCharSource(new File(pwFile), Charsets.UTF_8)
          .read().trim().toCharArray();
    } else {
      password = KEYSTORE_PASSWORD_DEFAULT;
    }

    // get local key file
    String keyFile = getConf().get(KEYSTORE_LOCAL_FILE_KEY,
        KEYSTORE_LOCAL_FILE_DEFAULT);
    localKeyFile = new File(keyFile);
    localKeyFilePrev = new File(keyFile + ".prev");

    ReadWriteLock lock = new ReentrantReadWriteLock(true);
    readLock = lock.readLock();
    writeLock = lock.writeLock();

    try {
      keyStore = KeyStore.getInstance(INTERNAL_SCHEME_NAME);
      if (loadKeys) {
        byte[] buf = loadFromZK();
        keyStore.load(buf.length > 0 ?
            new ByteArrayInputStream(buf) : null, password);
        LOG.info("Successfully loaded keys from "
            + znodeWorkingDir + " in ZK.");

        // init local key file
        if (localKeyFile.exists()) {
          FileUtils.copyFile(localKeyFile, localKeyFilePrev);
        }
        try (FileOutputStream fos = FileUtils.openOutputStream(localKeyFile)) {
          keyStore.store(fos, password);
        }
      }
    } catch (KeyStoreException e) {
      throw new IOException("Can't create keystore", e);
    } catch (NoSuchAlgorithmException e) {
      throw new IOException("Can't load keystore " + znodeWorkingDir, e);
    } catch (CertificateException e) {
      throw new IOException("Can't load keystore " + znodeWorkingDir, e);
    } catch (IOException e) {
      throw new IOException("Can't load keystore " + znodeWorkingDir, e);
    }
  }

  private String getParentZnode(String znode) {
    String[] pathParts = znode.replaceAll("//*", "/").split("/");
    Preconditions.checkArgument(pathParts.length >= 1 &&
        pathParts[0].isEmpty(), "Invalid parent znode: %s", znode);

    StringBuilder sb = new StringBuilder();
    for (int i = 1; i < pathParts.length; i++) {
      sb.append("/").append(pathParts[i]);
    }
    return sb.toString();
  }

  private void createConnection() throws IOException {
    if (zkClient != null) {
      try {
        zkClient.close();
      } catch (InterruptedException e) {
        throw new IOException("Interrupted while closing ZK", e);
      }
      zkClient = null;
      watcher = null;
    }
    zkClient = getNewZooKeeper();
    LOG.info("Successfully created new connection at " + zkHostPort);
  }

  /**
   * Get a new zookeeper client instance.
   *
   * @return new zookeeper client instance
   * @throws IOException
   */
  private ZooKeeper getNewZooKeeper() throws IOException {
    // Unfortunately, the ZooKeeper constructor connects to ZooKeeper and
    // may trigger the Connected event immediately. So, if we register the
    // watcher after constructing ZooKeeper, we may miss that event. Instead,
    // we construct the watcher first, and have it block any events it receives
    // before we can set its ZooKeeper reference.
    watcher = new WatcherWithClientRef();
    ZooKeeper zk = new ZooKeeper(zkHostPort, zkSessionTimeout, watcher);
    watcher.setZooKeeperRef(zk);

    // Wait for the asynchronous success/failure. This may throw an exception
    // if we don't connect within the session timeout.
    watcher.waitForZKConnectionEvent(zkSessionTimeout);

    for (ZKAuthInfo auth : zkAuthInfo) {
      zk.addAuthInfo(auth.getScheme(), auth.getAuth());
    }

    return zk;
  }

  /**
   * interface implementation of Zookeeper watch events (connection and node),
   * proxied by {@link WatcherWithClientRef}.
   */
  private void processWatchEvent(WatchedEvent event) {
    LOG.debug("Watcher event with state:"
        + event.getState() + " for path:" + event.getPath() + " for " + this);

    switch (event.getState()) {
    case SyncConnected:
      LOG.info("Session connected.");
      break;
    case Disconnected:
      // the connection got terminated because of session disconnect
      // call listener to reconnect
      LOG.info("Session disconnected. try to reconnect ...");
      reConnect();
      break;
    case Expired:
      // the connection got terminated because of session timeout
      // call listener to reconnect
      LOG.info("Session expired. try to reconnect ...");
      reConnect();
      break;
    default:
      LOG.error("Unexpected Zookeeper watch event state: " + event.getState());
      break;
    }
  }

  private void reConnect() {
    while (true) {
      LOG.debug("Establishing zookeeper connection for " + this);

      zkClientWLock.lock();
      try {
        terminateConnection();
        createConnection();
        break;
      } catch(IOException e) {
        LOG.error("Reconnect failed.", e);
      } finally {
        zkClientWLock.unlock();
      }

      sleepFor(5000);
    }
  }

  private void terminateConnection() {
    if (zkClient == null) {
      return;
    }
    LOG.debug("Terminating ZK connection for " + this);
    ZooKeeper tempZk = zkClient;
    zkClient = null;
    watcher = null;
    try {
      tempZk.close();
    } catch(InterruptedException e) {
      LOG.error("Terminate connection interrupted.", e);
    }
  }

  private void sleepFor(int sleepMs) {
    if (sleepMs > 0) {
      try {
        Thread.sleep(sleepMs);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * loading from the ZnodeWoringDir in ZooKeeper quorum.
   * @return the key file content
   * @throws IOException
   */
  private byte[] loadFromZK() throws IOException {
    // Get all znodes corresponding to the key file
    List<String> keyZnodes;
    try {
      keyZnodes = getChildrenWithRetries(znodeWorkingDir, false);
    } catch (KeeperException e) {
      throw new IOException("Couldn't get children of " + znodeWorkingDir, e);
    } catch (InterruptedException e) {
      throw new IOException("Thread interrupted while getting children of "
          + znodeWorkingDir, e);
    }

    // No existing znodes, return an empty array
    if (keyZnodes.isEmpty()) {
      return new byte[0];
    }

    // Sort znodes by name, eg. part_0000001, part_0000002, part_0000003
    Collections.sort(keyZnodes, String.CASE_INSENSITIVE_ORDER);

    // Get all the znodes data and combine them into a complete key file content
    ArrayList<Byte> keysContent = new ArrayList<Byte>();
    for (String znode : keyZnodes) {
      try {
        String path = new StringBuilder(znodeWorkingDir).append("/")
                          .append(znode).toString();
        byte[] tmpData = getDataWithRetries(path, false, null);
        keysContent.addAll(Arrays.asList(ArrayUtils.toObject(tmpData)));
      } catch (KeeperException e) {
        throw new IOException("Couldn't get data of " + znode, e);
      } catch (InterruptedException e) {
        throw new IOException("Thread interrupted while getting data of "
            + znode, e);
      }
    }

    // return key file content
    Byte[] tmpArray = new Byte[keysContent.size()];
    keysContent.toArray(tmpArray);
    return ArrayUtils.toPrimitive(tmpArray);
  }

  private char[] getPassword() throws IOException {
    return password;
  }

  private KeyStore getKeyStore() {
    return keyStore;
  }

  private String getWorkingDir() {
    return znodeWorkingDir;
  }

  @Override
  public KeyVersion getKeyVersion(String versionName) throws IOException {
    readLock.lock();
    try {
      SecretKeySpec key = null;
      try {
        if (!keyStore.containsAlias(versionName)) {
          return null;
        }
        key = (SecretKeySpec) keyStore.getKey(versionName, password);
      } catch (KeyStoreException e) {
        throw new IOException("Can't get key " + versionName + " from " +
                              znodeWorkingDir, e);
      } catch (NoSuchAlgorithmException e) {
        throw new IOException("Can't get algorithm for key " + key + " from " +
                              znodeWorkingDir, e);
      } catch (UnrecoverableKeyException e) {
        throw new IOException("Can't recover key " + key + " from " +
                              znodeWorkingDir, e);
      }
      return new KeyVersion(getBaseName(versionName), versionName,
          key.getEncoded());
    } catch (IOException ioe) {
      LOG.error(getErrorMessage(ioe));
      throw ioe;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public List<String> getKeys() throws IOException {
    readLock.lock();
    try {
      ArrayList<String> list = new ArrayList<String>();
      String alias = null;
      try {
        Enumeration<String> e = keyStore.aliases();
        while (e.hasMoreElements()) {
          alias = e.nextElement();
          // only include the metadata key names in the list of names
          if (!alias.contains("@")) {
            list.add(alias);
          }
        }
      } catch (KeyStoreException e) {
        throw new IOException("Can't get key " + alias + " from " +
                              znodeWorkingDir, e);
      }
      return list;
    } catch (IOException ioe) {
      LOG.error(getErrorMessage(ioe));
      throw ioe;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public List<KeyVersion> getKeyVersions(String name) throws IOException {
    readLock.lock();
    try {
      List<KeyVersion> list = new ArrayList<KeyVersion>();
      Metadata km = getMetadata(name);
      if (km != null) {
        int latestVersion = km.getVersions();
        KeyVersion v = null;
        String versionName = null;
        for (int i = 0; i < latestVersion; i++) {
          versionName = buildVersionName(name, i);
          v = getKeyVersion(versionName);
          if (v != null) {
            list.add(v);
          }
        }
      }
      return list;
    } catch (IOException ioe) {
      LOG.error(getErrorMessage(ioe));
      throw ioe;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public Metadata getMetadata(String name) throws IOException {
    readLock.lock();
    try {
      if (cache.containsKey(name)) {
        return cache.get(name);
      }
      try {
        if (!keyStore.containsAlias(name)) {
          return null;
        }
        Metadata meta = ((KeyMetadata) keyStore.getKey(name, password))
            .metadata;
        cache.put(name, meta);
        return meta;
      } catch (ClassCastException e) {
        throw new IOException("Can't cast key for " + name + " in keystore " +
            znodeWorkingDir + " to a KeyMetadata. Key may have been added using"
            + " keytool or some other non-Hadoop method.", e);
      } catch (KeyStoreException e) {
        throw new IOException("Can't get metadata for " + name +
            " from keystore " + znodeWorkingDir, e);
      } catch (NoSuchAlgorithmException e) {
        throw new IOException("Can't get algorithm for " + name +
            " from keystore " + znodeWorkingDir, e);
      } catch (UnrecoverableKeyException e) {
        throw new IOException("Can't recover key for " + name +
            " from keystore " + znodeWorkingDir, e);
      }
    } catch (IOException ioe) {
      LOG.error(getErrorMessage(ioe));
      throw ioe;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public KeyVersion createKey(String name, byte[] material,
                               Options options) throws IOException {
    Preconditions.checkArgument(name.equals(StringUtils.toLowerCase(name)),
        "Uppercase key names are unsupported: %s", name);

    writeLock.lock();
    try {
      try {
        if (keyStore.containsAlias(name) || cache.containsKey(name)) {
          throw new IOException("Key " + name + " already exists in " + this);
        }
      } catch (KeyStoreException e) {
        throw new IOException("Problem looking up key " + name + " in " + this,
            e);
      }
      Metadata meta = new Metadata(options.getCipher(), options.getBitLength(),
          options.getDescription(), options.getAttributes(), new Date(), 1);
      if (options.getBitLength() != 8 * material.length) {
        throw new IOException("Wrong key length. Required " +
            options.getBitLength() + ", but got " + (8 * material.length));
      }
      cache.put(name, meta);
      String versionName = buildVersionName(name, 0);
      return innerSetKeyVersion(name, versionName, material, meta.getCipher());
    } catch (IOException ioe) {
      LOG.error(getErrorMessage(ioe));
      throw ioe;
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public void deleteKey(String name) throws IOException {
    writeLock.lock();
    try {
      Metadata meta = getMetadata(name);
      if (meta == null) {
        throw new IOException("Key " + name + " does not exist in " + this);
      }
      for(int v=0; v < meta.getVersions(); ++v) {
        String versionName = buildVersionName(name, v);
        try {
          if (keyStore.containsAlias(versionName)) {
            keyStore.deleteEntry(versionName);
          }
        } catch (KeyStoreException e) {
          throw new IOException("Problem removing " + versionName + " from " +
              this, e);
        }
      }
      try {
        if (keyStore.containsAlias(name)) {
          keyStore.deleteEntry(name);
        }
      } catch (KeyStoreException e) {
        throw new IOException("Problem removing " + name + " from " + this, e);
      }
      cache.remove(name);
      changed = true;
    } catch (IOException ioe) {
      LOG.error(getErrorMessage(ioe));
      throw ioe;
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public KeyVersion rollNewVersion(String name,
                                    byte[] material) throws IOException {
    writeLock.lock();
    try {
      Metadata meta = getMetadata(name);
      if (meta == null) {
        throw new IOException("Key " + name + " not found");
      }
      if (meta.getBitLength() != 8 * material.length) {
        throw new IOException("Wrong key length. Required " +
            meta.getBitLength() + ", but got " + (8 * material.length));
      }
      int nextVersion = meta.addVersion();
      String versionName = buildVersionName(name, nextVersion);
      return innerSetKeyVersion(name, versionName, material, meta.getCipher());
    } catch (IOException ioe) {
      LOG.error(getErrorMessage(ioe));
      throw ioe;
    } finally {
      writeLock.unlock();
    }
  }

  private KeyVersion innerSetKeyVersion(String name, String versionName,
                    byte[] material, String cipher) throws IOException {
    try {
      keyStore.setKeyEntry(versionName, new SecretKeySpec(material, cipher),
          password, null);
    } catch (KeyStoreException e) {
      throw new IOException("Can't store key " + versionName + " in " + this,
          e);
    }
    changed = true;
    return new KeyVersion(name, versionName, material);
  }

  @Override
  public void flush() throws IOException {
    writeLock.lock();
    try {
      if (!changed) {
        return;
      }

      // backup local key file
      FileUtils.copyFile(localKeyFile, localKeyFilePrev);

      // Put all of the updates into the keystore
      for(Map.Entry<String, Metadata> entry: cache.entrySet()) {
        try {
          keyStore.setKeyEntry(entry.getKey(),
              new KeyMetadata(entry.getValue()), password, null);
        } catch (KeyStoreException e) {
          throw new IOException("Can't set metadata key " + entry.getKey(), e);
        }
      }

      // write and verify
      try {
        // write to local and verify
        writeToLocal();
        verifyLocal();
        LOG.info("Successfully wrote keys to " + localKeyFile);

        // write to zk and verify
        ByteArrayOutputStream tmpOut = new ByteArrayOutputStream();
        keyStore.store(tmpOut, password);
        writeToZK(tmpOut.toByteArray());
        verifyZK();
        LOG.info("Successfully wrote keys to " + znodeWorkingDir + " in ZK.");
      } catch (KeyStoreException e) {
        throw new IOException("Can't store keystore " + this, e);
      } catch (IOException e) {
        throw new IOException("Can't store keystore " + this, e);
      } catch (NoSuchAlgorithmException e) {
        throw new IOException("No such algorithm storing keystore " + this, e);
      } catch (CertificateException e) {
        throw new IOException("Certificate exception storing keystore"
            + this, e);
      }
    } catch (IOException ioe) {
      LOG.error(getErrorMessage(ioe));
      resetKeyStoreState();
      throw ioe;
    } finally {
      writeLock.unlock();
    }
  }

  private void writeToLocal() throws KeyStoreException, IOException,
      NoSuchAlgorithmException, CertificateException {
    try (FileOutputStream fos = FileUtils.openOutputStream(localKeyFile)) {
      keyStore.store(fos, password);
    }
  }

  private void verifyLocal() throws KeyStoreException, IOException,
      NoSuchAlgorithmException, CertificateException {
    KeyStore tmpKeyStore = KeyStore.getInstance(INTERNAL_SCHEME_NAME);
    try (FileInputStream fis = FileUtils.openInputStream(localKeyFile)) {
      tmpKeyStore.load(fis, password);
    }
  }

  private void verifyZK() throws KeyStoreException, IOException,
      NoSuchAlgorithmException, CertificateException {
    byte[] tmpBuf = loadFromZK();
    ByteArrayInputStream tmpBais = new ByteArrayInputStream(tmpBuf);
    KeyStore tmpKeyStore = KeyStore.getInstance(INTERNAL_SCHEME_NAME);
    tmpKeyStore.load(tmpBais, password);
  }

  private void writeToZK(byte[] buf) throws IOException {
    // Split keys content to multi part, each part represented by a single
    // znode and has a data size znodeDataSize
    Byte[] tmpArray = ArrayUtils.toObject(buf);
    int newZnodesNum = (tmpArray.length + znodeDataSize - 1) / znodeDataSize;
    List<Byte[]> newZnodes = new ArrayList<Byte[]>(newZnodesNum);
    for (int i = 0; i < newZnodesNum; i++) {
      int from = i * znodeDataSize;
      int to = from + znodeDataSize;
      if (to > tmpArray.length) {
        to = tmpArray.length;
      }
      newZnodes.add(Arrays.copyOfRange(tmpArray, from, to));
    }

    // Get already existing old znodes
    List<String> oldZnodes;
    try {
      oldZnodes = getChildrenWithRetries(znodeWorkingDir, false);
    } catch (KeeperException e) {
      throw new IOException("Couldn't get children of " + znodeWorkingDir +
          " while writing to ZK.", e);
    } catch (InterruptedException e) {
      throw new IOException("Thread interrupted while getting children of " +
          znodeWorkingDir + " while writing to ZK.", e);
    }

    // Sort old Znodes by name, eg. part-0000001, part-0000002, ...
    Collections.sort(oldZnodes, String.CASE_INSENSITIVE_ORDER);

    // Split updating of key file into multi OPs, these OPs need to
    // be transactionally completed
    int maxNewIndex = newZnodes.size() - 1;
    int maxOldIndex = oldZnodes.size() - 1;
    int maxIndex = Math.max(maxNewIndex, maxOldIndex);
    final List<Op> execOpList = new ArrayList<Op>(maxIndex + 1);
    for (int i = 0; i <= maxIndex; i++) {
      String path = new StringBuilder(znodeWorkingDir).append('/')
              .append(ZK_KEY_ZNODE_NAME_PREFIX)
              .append(String.format("%09d", i + 1)).toString();
      byte[] data = (i <= maxNewIndex ?
              ArrayUtils.toPrimitive(newZnodes.get(i)) : new byte[] {});

      if (i <= maxNewIndex && i <= maxOldIndex) {
        // in new, and also in old, setData
        execOpList.add(Op.setData(path, data, -1));
      } else if (i <= maxNewIndex && i > maxOldIndex) {
        // in new, but not in old, create
        execOpList.add(Op.create(path, data, zkAcl, CreateMode.PERSISTENT));
      } else if (i > maxNewIndex && i <= maxOldIndex) {
        // not in new, but in old, delete
        execOpList.add(Op.delete(path, -1));
      } else if (i > maxNewIndex && i > maxOldIndex) {
        return; // not in new, and not in old, not possible
      }
    }

    // Exec op list with retries
    try {
      mutliWithRetries(execOpList);
    } catch (KeeperException e) {
      throw new IOException("Exec multi ops failed while writing to zk.", e);
    } catch (InterruptedException e) {
      throw new IOException("Thread interrupted while multi ops writing to zk.",
          e);
    }
  }

  private void resetKeyStoreState() {
    LOG.info("Could not flush Keystore.."
        + "attempting to reset to previous state !!");
    // 1) flush cache
    cache.clear();

    // 2) revert local keyStore, local key file, zookeeper key file
    try {
      // revert local keyStore
      try (FileInputStream fis = FileUtils.openInputStream(localKeyFilePrev)) {
        keyStore.load(fis, password);
        LOG.info("Local keyStore resetting to previously flushed state !!");
      }

      // revert local key file
      FileUtils.copyFile(localKeyFilePrev, localKeyFile);
      LOG.info("Local key file resetting to previously flushed state !!");

      // revert zookeeper key file
      byte[] buf = FileUtils.readFileToByteArray(localKeyFilePrev);
      writeToZK(buf);
      LOG.info("Zookeeper key file resetting to previously flushed state !!");
    } catch (Exception e) {
      LOG.warn("Could not reset all targets (local key store, local key file" +
          " and zookeeper key file) to previous state !!", e);
    }
  }

  @Override
  public String toString() {
    return zkHostPort + znodeWorkingDir;
  }

  private String getErrorMessage(Throwable t) {
    StringBuilder sb = new StringBuilder(ExceptionUtils.getStackTrace(t));
    while (t.getCause() != null) {
      sb.append("Caused by: ");
      sb.append(ExceptionUtils.getStackTrace(t.getCause()));
      t = t.getCause();
    }
    return sb.toString();
  }

  /**
   * Watcher implementation which keeps a reference around to the
   * original ZK connection, and passes it back along with any
   * events.
   */
  private final class WatcherWithClientRef implements Watcher {
    private ZooKeeper zk;
    /**
     * Latch fired whenever any event arrives. This is used in order
     * to wait for the Connected event when the client is first created.
     */
    private CountDownLatch hasReceivedEvent = new CountDownLatch(1);

    /**
     * Latch used to wait until the reference to ZooKeeper is set.
     */
    private CountDownLatch hasSetZooKeeper = new CountDownLatch(1);

    /**
     * Waits for the next event from ZooKeeper to arrive.
     *
     * @throws IOException if interrupted while connecting to ZooKeeper
     */
    private void waitForZKConnectionEvent(int connectionTimeoutMs)
        throws IOException {
      try {
        if (!hasReceivedEvent.await(connectionTimeoutMs,
            TimeUnit.MILLISECONDS)) {
          String errMsg = "Couldn't establish connection to ZooKeeper "
              + "in " + connectionTimeoutMs  + " milliseconds.";
          zk.close();
          throw new IOException(errMsg);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(
            "Interrupted when connecting to zookeeper server", e);
      }
    }

    private void setZooKeeperRef(ZooKeeper zoo) {
      Preconditions.checkState(zk == null,
          "zk already set -- must be set exactly once");
      zk = zoo;
      hasSetZooKeeper.countDown();
    }

    @Override
    public void process(WatchedEvent event) {
      hasReceivedEvent.countDown();
      try {
        if (!hasSetZooKeeper.await(zkSessionTimeout, TimeUnit.MILLISECONDS)) {
          LOG.debug("Event received with stale zk");
          return;
        }

        ZooKeeperKeyStoreProvider.this.processWatchEvent(event);
      } catch (Throwable t) {
        LOG.error("Failed to process watcher event " + event + ": " +
            StringUtils.stringifyException(t));
      }
    }
  }

  private Stat existsWithRetries(final String path, final boolean watch)
        throws InterruptedException, KeeperException {
    return zkDoWithRetries(new ZKAction<Stat>() {
      @Override
      public Stat run() throws KeeperException, InterruptedException {
        return zkClient.exists(path, watch);
      }
    });
  }

  private String createWithRetries(final String path, final byte[] data,
      final List<ACL> acl, final CreateMode mode)
      throws InterruptedException, KeeperException {
    return zkDoWithRetries(new ZKAction<String>() {
      @Override
      public String run() throws KeeperException, InterruptedException {
        return zkClient.create(path, data, acl, mode);
      }
    });
  }

  private byte[] getDataWithRetries(final String path, final boolean watch,
      final Stat stat) throws InterruptedException, KeeperException {
    return zkDoWithRetries(new ZKAction<byte[]>() {
      @Override
      public byte[] run() throws KeeperException, InterruptedException {
        return zkClient.getData(path, watch, stat);
      }
    });
  }

  private List<String> getChildrenWithRetries(final String path,
      final boolean watch) throws InterruptedException, KeeperException {
    return zkDoWithRetries(new ZKAction<List<String>>() {
      @Override
      public List<String> run() throws KeeperException, InterruptedException {
        return zkClient.getChildren(path, watch);
      }
    });
  }

  private  List<OpResult> mutliWithRetries(final Iterable<Op> ops)
      throws InterruptedException, KeeperException {
    return zkDoWithRetries(new ZKAction<List<OpResult>>() {
      @Override
      public List<OpResult> run() throws KeeperException, InterruptedException {
        return zkClient.multi(ops);
      }
    });
  }

  private <T> T zkDoWithRetries(ZKAction<T> action) throws KeeperException,
      InterruptedException {
    int retry = 1;

    while (true) {
      zkClientRLock.lock();
      try {
        if (zkClient == null) {
          throw KeeperException.create(Code.CONNECTIONLOSS, "ZK client"
              + " is unexpected null, may be it has already disconneted.");
        }
        return action.run();
      } catch (KeeperException ke) {
        if (shouldRetry(ke.code()) && ++retry < maxRetryNum) {
          continue;
        }
        throw ke;
      } finally {
        zkClientRLock.unlock();
      }
    }
  }

  private interface ZKAction<T> {
    T run() throws KeeperException, InterruptedException;
  }

  private boolean shouldRetry(Code code) {
    return code == Code.CONNECTIONLOSS || code == Code.OPERATIONTIMEOUT;
  }

  /**
   * @return true if the configured parent znode exists
   */
  private boolean parentZNodeExists() throws IOException, InterruptedException {
    try {
      return existsWithRetries(znodeWorkingDir, false) != null;
    } catch (KeeperException e) {
      throw new IOException("Couldn't determine existence of znode '" +
          znodeWorkingDir + "'", e);
    }
  }

  /**
   * Utility function to ensure that the configured base znode exists.
   * This recursively creates the znode as well as all of its parents.
   */
  private void ensureParentZNode() throws IOException, InterruptedException {
    String[] pathParts = znodeWorkingDir.split("/");
    Preconditions.checkArgument(pathParts.length >= 1 &&
        pathParts[0].isEmpty(), "Invalid path: %s", znodeWorkingDir);

    StringBuilder sb = new StringBuilder();
    for (int i = 1; i < pathParts.length; i++) {
      sb.append("/").append(pathParts[i]);
      final String prefixPath = sb.toString();
      LOG.debug("Ensuring existence of " + prefixPath);
      try {
        createWithRetries(prefixPath, new byte[]{}, zkAcl,
            CreateMode.PERSISTENT);
      } catch (KeeperException e) {
        if (e.code() == Code.NODEEXISTS) {
          // This is OK - just ensuring existence.
          continue;
        } else {
          throw new IOException("Couldn't create " + prefixPath, e);
        }
      }
    }

    LOG.info("Successfully created " + znodeWorkingDir + " in ZK.");
  }

  /**
   * Clear all of the state held within the parent ZNode.
   * This recursively deletes everything within the znode as well as the
   * parent znode itself.
   */
  private void clearParentZNode() throws IOException, InterruptedException {
    try {
      LOG.info("Recursively deleting " + znodeWorkingDir + " from ZK...");

      zkDoWithRetries(new ZKAction<Void>() {
        @Override
        public Void run() throws KeeperException, InterruptedException {
          org.apache.zookeeper.ZKUtil.deleteRecursive(zkClient,
              znodeWorkingDir);
          return null;
        }
      });
    } catch (KeeperException e) {
      throw new IOException("Couldn't clear parent znode " + znodeWorkingDir,
          e);
    }
    LOG.info("Successfully deleted " + znodeWorkingDir + " from ZK.");
  }

  private boolean confirmFormat() {
    String parentZnode = znodeWorkingDir;
    System.err.println(
        "===============================================\n" +
        "The configured parent znode " + parentZnode + " already exists.\n" +
        "Are you sure you want to clear all information from ZooKeeper?\n" +
        "WARNING: Before proceeding, ensure that all servers using this \n" +
        "parent znode are stopped!\n" +
        "===============================================");
    try {
      return ToolRunner.confirmPrompt("Proceed formatting "
          + parentZnode + "?");
    } catch (IOException e) {
      LOG.debug("Failed to confirm", e);
      return false;
    }
  }

  /**
   * Formats the storage directory for the keys.
   * currently only can be used by admin cli.
   */
  public static synchronized int formatKeyStore(URI uri, Configuration conf,
      boolean force, boolean interactive)
          throws IOException, InterruptedException {
    ZooKeeperKeyStoreProvider zkp = new ZooKeeperKeyStoreProvider(uri,
        conf, false);
    if (zkp.parentZNodeExists()) {
      if (!force && (!interactive || !zkp.confirmFormat())) {
        return 1;
      }
      zkp.clearParentZNode();
    }

    zkp.ensureParentZNode();
    return 0;
  }

  /**
   * Import the key storage content from the a local file to zk cluster.
   * currently only can be used by admin cli.
   */
  public static synchronized int importKeyStore(URI uri, Configuration conf,
      String path) throws IOException {
    File f = new File(path);
    byte[] buf = FileUtils.readFileToByteArray(f);

    ZooKeeperKeyStoreProvider zkp = new ZooKeeperKeyStoreProvider(uri,
        conf, false);
    try {
      // need to verify local file first, by calling keyStore.load()
      zkp.getKeyStore().load(new ByteArrayInputStream(buf), zkp.getPassword());
      zkp.writeToZK(buf);
    } catch (NoSuchAlgorithmException e) {
      throw new IOException("Key file " + path + " is illegal, no keys "
          + "imported, original keys in ZooKeeper have not been modified.", e);
    } catch (CertificateException e) {
      throw new IOException("Key file " + path + " is illegal, no keys "
          + "imported, original keys in ZooKeeper have not been modified.", e);
    } catch (IOException e) {
      throw new IOException("Import failed, no keys imported,"
          + " original keys in ZooKeeper have not been modified.", e);
    }

    LOG.info("Successfully import keys from " + path + " to ZooKeeper "
              + zkp.getWorkingDir() + ".");
    return 0;
  }

  /**
   * The factory to create ZooKeeperKeyProvider, which is used by ServiceLoader.
   */
  public static class Factory extends KeyProviderFactory {
    @Override
    public KeyProvider createProvider(URI providerName,
                                      Configuration conf) throws IOException {
      if (SCHEME_NAME.equals(providerName.getScheme())) {
        return new ZooKeeperKeyStoreProvider(providerName, conf);
      }
      return null;
    }
  }

  /**
   * An adapter between a KeyStore Key and our Metadata. This is used to store
   * the metadata in a KeyStore even though isn't really a key.
   */
  public static final class KeyMetadata implements Key, Serializable {
    private Metadata metadata;
    private static final long serialVersionUID = 7405872419967874451L;

    private KeyMetadata(Metadata meta) {
      this.metadata = meta;
    }

    @Override
    public String getAlgorithm() {
      return metadata.getCipher();
    }

    @Override
    public String getFormat() {
      return KEY_METADATA;
    }

    @Override
    public byte[] getEncoded() {
      return new byte[0];
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
      byte[] serialized = metadata.serialize();
      out.writeInt(serialized.length);
      out.write(serialized);
    }

    private void readObject(ObjectInputStream in
                            ) throws IOException, ClassNotFoundException {
      byte[] buf = new byte[in.readInt()];
      in.readFully(buf);
      metadata = new Metadata(buf);
    }
  }
}