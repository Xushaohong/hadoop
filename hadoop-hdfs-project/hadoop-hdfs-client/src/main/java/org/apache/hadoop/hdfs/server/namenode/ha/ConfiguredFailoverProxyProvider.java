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
package org.apache.hadoop.hdfs.server.namenode.ha;

import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URI;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.ipc.RPC;

/**
 * A FailoverProxyProvider implementation which allows one to configure
 * multiple URIs to connect to during fail-over. A random configured address is
 * tried first, and on a fail-over event the other addresses are tried
 * sequentially in a random order.
 */
public class ConfiguredFailoverProxyProvider<T> extends
    AbstractNNFailoverProxyProvider<T> {

  protected final List<NNProxyInfo<T>> proxies;

  private int currentProxyIndex = 0;
  private boolean cacheActiveEnabled;
  private File cacheActiveFile;
  private static final Object FILE_SYNC_OBJ = new Object();

  public ConfiguredFailoverProxyProvider(Configuration conf, URI uri,
      Class<T> xface, HAProxyFactory<T> factory) {
    this(conf, uri, xface, factory, DFS_NAMENODE_RPC_ADDRESS_KEY);
  }

  public ConfiguredFailoverProxyProvider(Configuration conf, URI uri,
      Class<T> xface, HAProxyFactory<T> factory, String addressKey) {
    super(conf, uri, xface, factory);
    this.proxies = getProxyAddresses(uri, addressKey);
    this.cacheActiveEnabled = this.conf.getBoolean(
        HdfsClientConfigKeys.Failover.CACHE_ACTIVE_ENABLED_KEY,
        HdfsClientConfigKeys.Failover.CACHE_ACTIVE_ENABLED_DEFAULT);
    String cacheActiveDir = this.conf.get(
        HdfsClientConfigKeys.Failover.CACHE_ACTIVE_DIR_KEY,
        HdfsClientConfigKeys.Failover.CACHE_ACTIVE_DIR_DEFAULT);
    this.cacheActiveFile = new File(cacheActiveDir, uri.getHost());
    this.currentProxyIndex = getActiveIndex();
  }

  /**
   * Lazily initialize the RPC proxy object.
   */
  @Override
  public synchronized ProxyInfo<T> getProxy() {
    NNProxyInfo<T> current = proxies.get(currentProxyIndex);
    return createProxyIfNeeded(current);
  }

  @Override
  public  void performFailover(T currentProxy) {
    incrementProxyIndex();
    writeActiveCache();
  }

  synchronized void incrementProxyIndex() {
    currentProxyIndex = (currentProxyIndex + 1) % proxies.size();
  }

  /**
   * Close all the proxy objects which have been opened over the lifetime of
   * this proxy provider.
   */
  @Override
  public synchronized void close() throws IOException {
    for (ProxyInfo<T> proxy : proxies) {
      if (proxy.proxy != null) {
        if (proxy.proxy instanceof Closeable) {
          ((Closeable)proxy.proxy).close();
        } else {
          RPC.stopProxy(proxy.proxy);
        }
      }
    }
  }

  /**
   * Logical URI is required for this failover proxy provider.
   */
  @Override
  public boolean useLogicalURI() {
    return true;
  }

  /**
   * Write active NameNode to cache file.
   */
  private void writeActiveCache() {
    if (!cacheActiveEnabled) {
      return;
    }

    String currentActive =
        proxies.get(currentProxyIndex).getAddress().getHostString();

    synchronized (FILE_SYNC_OBJ) {
      try (RandomAccessFile raf = new RandomAccessFile(cacheActiveFile, "rw");
          FileChannel fc = raf.getChannel();
          FileLock lock = fc.tryLock(0, Long.MAX_VALUE, false)) {
        if (lock != null) {
          raf.setLength(0);
          raf.writeBytes(currentActive);
          boolean ret = cacheActiveFile.setWritable(true, false)
                        && cacheActiveFile.setReadable(true, false);
          if (!ret) {
            throw new IOException("Cannot set file rw mode.");
          }
          LOG.debug("Succeed in writing current active NameNode "
              + currentActive + " to cache file " + cacheActiveFile);
        }
      } catch (Throwable e) {
        LOG.warn("Failed to write current active NameNode " + currentActive
            + " to cache file " + cacheActiveFile + ", It may be due to "
            + "permission issues, the dfs.client.failover.cache-active.dir "
            + "must be properly configured!", e);
      }
    }
  }

  /**
   * Read active NameNode from cache file.
   */
  private int getActiveIndex() {
    if (!cacheActiveEnabled) {
      return 0;
    }

    String currentActive = null;
    int activeindex = 0;

    synchronized (FILE_SYNC_OBJ) {
      if (!cacheActiveFile.exists()) {
        return 0;
      }

      try (RandomAccessFile raf = new RandomAccessFile(cacheActiveFile, "rw");
          FileChannel fc = raf.getChannel();
          FileLock lock = fc.tryLock(0, Long.MAX_VALUE, true)) {
        if (lock != null) {
          currentActive = raf.readLine();
        }
      } catch (Throwable e) {
        LOG.warn("Failed to read active NameNode from cache file "
            + cacheActiveFile + ", It may be due to permission issues, the "
            + "dfs.client.failover.cache-active.dir must be properly "
            + "configured! Now we will begin from index 0.", e);
        return 0;
      }
    }

    for (int i = 0; i < proxies.size(); i++) {
      if (proxies.get(i).getAddress().getHostString().equals(currentActive)) {
        activeindex = i;
        break;
      }
    }
    return activeindex;
  }
}
