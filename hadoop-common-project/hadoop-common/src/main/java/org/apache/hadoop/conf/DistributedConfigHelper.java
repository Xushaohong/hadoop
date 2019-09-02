/*
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
package org.apache.hadoop.conf;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.Reader;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ThreadUtil;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.http.conn.util.InetAddressUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Joiner.MapJoiner;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * load resource from remote.
 */
public final class DistributedConfigHelper {
  private static final String DISTIRBUTED_BUILT_CONFIG =
      "distirbuted.built.config";

  private static final int DEFAULT_ZK_SESSION_TIMEOUT_IN_MILLIS = 60 * 1000;

  public static final String HDFS = "hdfs";

  public static final String ZK_PATH_SPLITER = "/";

  public static final String REMOTE_BASE_PATH = "/hadoop-distributed-config";

  private static final String COLON = ":";

  private static final String COMMA = ",";

  public static final String VERSION = "VERSION";

  public static final Log LOG =
      LogFactory.getLog(DistributedConfigHelper.class);

  public final static String CONFIG_ZK_QUORUM_KEY = "config.zookeeper.quorum";

  public final static String DISTRIBUTED_CONFIG_PREFERRED_KEY =
      "distributed.config.preferred";

  public final static String CONFIG_LOAD_MODE_KEY = "config.load.mode";

  public final static String CONFIG_DIR_KEY = "local.config.dir";

  public final static String DISTRIBUTED_CONFIG_ENABLE_KEY =
      "distributed.config.enable";

  private final static String XML_SUFFIX = ".xml";

  public final static String CHARSET = "UTF-8";

  private final static String LOCK_FILE = "cfg.lock";

  private static final List<ACL> ALL_ACL_LIST =
      Arrays.asList(new ACL(ZooDefs.Perms.ALL, new Id("world", "anyone")));

  private static DistributedConfigHelper instance;

  private DistributedConfigHelper() {
  }

  public static DistributedConfigHelper get() {
    // ignore thread unsafe
    if (instance == null) {
      instance = new DistributedConfigHelper();
    }
    return instance;
  }

  /**
   * build configuration.
   *
   * @see #build(String, Configuration, String...)
   * @param nameNodeUri
   * @param configurationClass
   * @param zkQuorum if null,will use local snapshot resource if exists
   * @param snapshotDir
   * @param loadMode
   * @param resources
   */
  public <CONFIG extends Configuration> CONFIG build(String nameNodeUri,
      Class<CONFIG> configurationClass, String zkQuorum, String snapshotDir,
      LoadMode loadMode, String... resources) {
    CONFIG configuration = ReflectionUtils.newInstance(
        configurationClass, null);
    configuration.set(DISTRIBUTED_CONFIG_ENABLE_KEY, Boolean.TRUE.toString());
    configuration.set(CONFIG_DIR_KEY, snapshotDir);
    configuration.set(CONFIG_ZK_QUORUM_KEY, zkQuorum);
    configuration.set(CONFIG_LOAD_MODE_KEY, loadMode.toString());
    return build(nameNodeUri, configuration, resources);
  }

  /**
   * build configuration with zookeeper resource.
   *
   * @param nameNodeUri
   *            namenode uri
   * @param configuration
   *            config instance
   * @param resources
   *            specified resource,support "[all|*]"
   * @return build with specified resources's configuration
   */
  public <CONFIG extends Configuration> CONFIG build(String nameNodeUri,
      CONFIG configuration, String... resources) {
    URI nnUri = null;
    if (StringUtils.isBlank(nameNodeUri) && configuration != null) {
      nnUri = FileSystem.getDefaultUri(configuration);
    } else {
      try {
        nnUri = new URI(nameNodeUri);
      } catch (URISyntaxException e) {
        LOG.error("namenode uri is illegal:" + nameNodeUri, e);
        return configuration;
      }
    }
    return build(nnUri, configuration, resources);
  }

  public synchronized <CONFIG extends Configuration> CONFIG build(
      URI nameNodeUri, CONFIG configuration, String... resources) {
    long start = System.currentTimeMillis();
    String host = null;
    String zkQuorum = null;
    File localConfigDir = null;
    ZooKeeperHolder holder = null;
    boolean isAll = false;
    LoadMode loadMode = null;
    LocalFileLock fileLock = null;
    Set<String> builtRs = new HashSet<String>();
    Set<String> resourceSet = new HashSet<String>();

    if (configuration == null) {
      LOG.info("configuration is null.");
      return configuration;
    }

    Configuration localConf = new Configuration();
    if (!Boolean.valueOf(getProperty(localConf, DISTRIBUTED_CONFIG_ENABLE_KEY,
        getProperty(configuration, DISTRIBUTED_CONFIG_ENABLE_KEY,
            Boolean.FALSE.toString())))) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("distirbuted config is disable.");
      }
      return configuration;
    }
    if(ArrayUtils.isEmpty(resources)) {
      resources = new String[] {"core-site.xml", "hdfs-site.xml"};
    }

    isAll = filter(Arrays.asList(resources), resourceSet);

    if (configuration.get(DISTIRBUTED_BUILT_CONFIG) != null) {
      builtRs.addAll(Arrays.asList(configuration.
          getStrings(DISTIRBUTED_BUILT_CONFIG)));
    }

    if (!builtRs.isEmpty()) {
      if (isBuilt(builtRs, resourceSet)) {
        LOG.info("configuration is built"+ (resourceSet.isEmpty() ? ""
            : resourceSet.toString()));
        return configuration;
      }
      resourceSet.removeAll(builtRs);
    }

    host = nameNodeUri.getHost();
    if (nameNodeUri == null || !HDFS.equals(nameNodeUri.getScheme())
        || host == null) {
      LOG.info("NameNode URI is NULL or host is defective:" + nameNodeUri);
      return configuration;
    }

    if (InetAddressUtils.isIPv4Address(host)) {
      LOG.info("NameNode URI is not HA mode:" + nameNodeUri);
      return configuration;
    }

    String loadModeValue = getProperty(localConf, CONFIG_LOAD_MODE_KEY, null);
    if (loadModeValue != null) {
      loadMode = LoadMode.valueOf(loadModeValue);
    }

    if (loadMode == null) {
      loadMode = LoadMode.LOCAL;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("load mode:" + loadMode);
    }

    zkQuorum = getProperty(localConf, CONFIG_ZK_QUORUM_KEY,
        getProperty(configuration, CONFIG_ZK_QUORUM_KEY, null));

    if (zkQuorum == null && loadMode != LoadMode.LOCAL) {
      LOG.warn("configuration not spacial zookeeper quorum, "
          + "will use local snapshot resource if not ignore");
    }

    String rootDir = getProperty(localConf, CONFIG_DIR_KEY, getTmpConfDir());
    if(!checkLocalPath(new File(rootDir))){
      LOG.warn("dir not exists or not have permission:" + rootDir);
      return configuration;
    }
    localConfigDir = new File(rootDir, host);
    if(!checkLocalPath(localConfigDir)){
      LOG.warn("dir not exists or not have permission:" + localConfigDir);
      return configuration;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("before build configuration:"
          + ReflectionToStringBuilder.toStringExclude(configuration,
          new String[] {"updatingResource"}));
    }

    fileLock = new LocalFileLock(new File(localConfigDir, LOCK_FILE),
        LoadMode.LOCAL == loadMode ? true : false);
    try {
      fileLock.lock();
      if (StringUtils.isNotBlank(zkQuorum)) {
        holder = ZooKeeperHolder.init(zkQuorum, localConf);
      }

      Map<String, InputStream> inputStreams = getResourceStream(host,
          localConfigDir, holder, isAll, resourceSet, loadMode, fileLock);
      if (MapUtils.isNotEmpty(inputStreams)) {
        if (configuration.getBoolean(DISTRIBUTED_CONFIG_PREFERRED_KEY, true)){
          Configuration tmpCfg = new Configuration(false);
          for (Entry<String, InputStream> entry : inputStreams.entrySet()) {
            tmpCfg.addResource(entry.getValue(),
                "distributed." + entry.getKey());
          }
          Iterator<Entry<String, String>> iterator = tmpCfg.iterator();
          while (iterator.hasNext()) {
            Entry<String, String> pair = iterator.next();
            configuration.set(pair.getKey(), pair.getValue());
          }
        } else {
          for (Entry<String, InputStream> entry : inputStreams.entrySet()) {
            configuration.addResource(entry.getValue(),
                "distributed." + entry.getKey());
          }
        }

      }
      if (isAll) {
        builtRs.add("*");
      } else {
        builtRs.addAll(resourceSet);
      }
      configuration.set(DISTIRBUTED_BUILT_CONFIG,
          Joiner.on(",").join(builtRs.iterator()).toString());
    } finally {
      fileLock.unlock();
      if (holder != null) {
        holder.close();
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("build configuration finished,cost:" +
          (System.currentTimeMillis() - start) + " ms");
      LOG.debug("after build configuration:"
          + ReflectionToStringBuilder.toStringExclude(configuration,
          new String[] {"updatingResource"}));
      dump(host, localConfigDir.getParentFile(), configuration);
    }

    return configuration;
  }

  private boolean isBuilt(Set<String> builtRs, Set<String> resources) {
    if (CollectionUtils.isNotEmpty(builtRs)) {
      if (builtRs.contains("*")) {
        return true;
      }
      return CollectionUtils.isNotEmpty(resources)
          && builtRs.containsAll(resources);
    }
    return false;
  }

  private String getTmpConfDir() {
    return File.separator + "tmp" + File.separator + "dist-config";
  }

  private Map<String, InputStream> getResourceStream(String host,
      File localConfigDir, ZooKeeperHolder holder, boolean isAll,
      final Set<String> resourceSet, LoadMode loadMode,
      LocalFileLock fileLock) {
    boolean ignoreLocal = false;
    // process extra situation
    if (LoadMode.LOCAL == loadMode) {
      if (isAll && !existsAnyResources(localConfigDir) && holder.connect()) {
        LOG.info("not exists local resources,change mode from "
            + LoadMode.LOCAL + " to " + LoadMode.REMOTE
            + ", path:" + localConfigDir.getPath());
        loadMode = LoadMode.REMOTE;
        fileLock.upgradeLock();
      }
      if (!isAll && !existsSpecResources(localConfigDir, resourceSet, true)
          && holder.connect()) {
        LOG.info("not exists  all special local resources,change mode from "
            + LoadMode.LOCAL + " to " + LoadMode.MIX
            + ", path:" + localConfigDir.getPath());
        loadMode = LoadMode.MIX;
        fileLock.upgradeLock();
      }
    } else {
      if (!holder.connect()) {
        if (LoadMode.REMOTE == loadMode) {
          LOG.info("load mode is " + loadMode
              + ", but can't connnect to zooKeeper");
          return Collections.emptyMap();
        }

        if (LoadMode.MIX == loadMode) {
          if (!existsSpecResources(localConfigDir, resourceSet, false)) {
            LOG.info("load mode is " + loadMode
                + ", can't connnect to zooKeeper and not exists"
                + "any local resource");
            return Collections.emptyMap();
          }
          LOG.info("can't connect to zookeeper,change load mode "
              + "to " + LoadMode.LOCAL);
          loadMode = LoadMode.LOCAL;
          fileLock.demoteLock();
        }
      }
    }

    Map<String, InputStream> inputStreams = null;
    switch (loadMode) {
    case LOCAL:
      Map<String, File> localConfigFiles =
          getLocalFile(localConfigDir, resourceSet, isAll);
      if (!isAll) {
        Set<String> missing = Sets.difference(resourceSet,
            localConfigFiles.keySet());
        if (CollectionUtils.isNotEmpty(missing) && LOG.isDebugEnabled()) {
          LOG.info("loadMode:" + loadMode + "fetch resources:" + resourceSet
              + ", load resources:" + localConfigFiles.keySet() + ", missing "
              + missing);
        }
      }
      inputStreams = loadLocalResource(localConfigFiles.values());
      break;

    case REMOTE:
      ignoreLocal = true;
    case MIX:
      inputStreams = loadResource(holder, host, localConfigDir, isAll,
          resourceSet, ignoreLocal);
      LOG.info("load resources:" + resourceSet + ", load mode:" + loadMode);
      break;

    default:
      break;
    }

    return inputStreams;
  }

  private String getProperty(Configuration configuration, String key,
      String defaultValue) {
    String value = "";
    if (configuration != null) {
      value = configuration.get(key);
    }
    if (StringUtils.isBlank(value)) {
      value = System.getProperty(key);
      return StringUtils.isNotBlank(value) ? value : defaultValue;
    }
    return value;
  }

  public boolean uploadResource(Configuration config, String... resources) {
    String nameNodeUri = config.get(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY,
        CommonConfigurationKeys.FS_DEFAULT_NAME_DEFAULT);
    String quorum = config.get(CONFIG_ZK_QUORUM_KEY);
    String dir = config.get(CONFIG_DIR_KEY);
    if (nameNodeUri == null || quorum == null || dir == null) {
      LOG.warn("config is defective,"
          + CommonConfigurationKeys.FS_DEFAULT_NAME_KEY + ":" + nameNodeUri
          + "," + CONFIG_ZK_QUORUM_KEY + ":" + quorum + "," + CONFIG_DIR_KEY
          + ":" + dir);
      return false;
    }
    return uploadResource(nameNodeUri, quorum, dir, config, resources);
  }

  public boolean dropResource(Configuration config, String... resources) {
    String nameNodeUri = config.get(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY,
        CommonConfigurationKeys.FS_DEFAULT_NAME_DEFAULT);
    String quorum = config.get(CONFIG_ZK_QUORUM_KEY);
    if (nameNodeUri == null || quorum == null) {
      LOG.warn("config is defective,"
          + CommonConfigurationKeys.FS_DEFAULT_NAME_KEY + ":" + nameNodeUri
          + "," + CONFIG_ZK_QUORUM_KEY + ":" + quorum);
      return false;
    }
    return dropResource(nameNodeUri, quorum, config, resources);
  }

  /**
   * upload specified resource to remote.
   *
   * @param nameNodeUri
   * @param zkQuorum
   * @param confDir
   * @param resources
   * @throws Exception
   */
  public boolean uploadResource(String nameNodeUri, String zkQuorum,
      String confDir, Configuration config, final String... resources) {
    long start = System.currentTimeMillis();
    URI nnUri = null;
    ZooKeeperHolder holder = null;
    try {
      nnUri = new URI(nameNodeUri);
    } catch (URISyntaxException e) {
      String message = "namenode uri is illegal:" + nameNodeUri;
      LOG.error(message, e);
      return false;
    }

    if (!HDFS.equals(nnUri.getScheme())) {
      String message = "protocol not hdfs,actual:" + nnUri.getScheme();
      LOG.error(message);
      return false;
    }

    if (StringUtils.isEmpty(nnUri.getHost())) {
      LOG.warn("host is null");
      return false;
    }

    holder = ZooKeeperHolder.init(zkQuorum, config);

    try {
      if (!holder.connect()) {
        LOG.warn("can't connect zk,querom:" + zkQuorum);
        return false;
      }

      LOG.info("create zookeeper,quorum:" + zkQuorum);
      final long timestamp = System.currentTimeMillis();

      File localConfigDir = new File(confDir);
      if (!checkLocalPath(localConfigDir, false)) {
        String message = "resouce dir can't read:" + confDir;
        LOG.error(message);
        return false;
      }

      final Set<String> rsSet = new HashSet<String>();

      final boolean isAll = filter(resources != null ? Arrays.asList(resources)
          : null, rsSet);

      Map<String, File> rsFilesMap = getLocalFile(localConfigDir, rsSet, isAll);

      if (rsFilesMap.isEmpty()
          || (!isAll && rsFilesMap.size() != rsSet.size())) {
        LOG.error("not found resource files,parent dir:" + confDir
            + (isAll ? " " : ", missing:"
            + Sets.difference(rsSet, rsFilesMap.keySet())));
        return false;
      }

      LOG.info("found resources:" + rsFilesMap.keySet());

      final String path = REMOTE_BASE_PATH + ZK_PATH_SPLITER + nnUri.getHost();
      String versionPath = path + ZK_PATH_SPLITER + VERSION;

      boolean exists = false;

      if (holder.get().exists(REMOTE_BASE_PATH, true) == null) {
        holder.get().create(REMOTE_BASE_PATH, null, ALL_ACL_LIST,
            CreateMode.PERSISTENT);
        LOG.info(" not exists path:" + REMOTE_BASE_PATH
            + " on zk,create firstly");
      }

      exists = (holder.get().exists(path, true) != null);
      if (!exists) {
        holder.get().create(path, null, ALL_ACL_LIST, CreateMode.PERSISTENT);

        Iterator<String> timestampIterator =
            Iterators.transform(rsFilesMap.keySet().iterator(),
                new Function<String, String>() {
                @Override
                public String apply(String input) {
                  return input + COLON + timestamp;
                }
              });
        String timestamps = Joiner.on(COMMA).join(timestampIterator).toString();

        holder.get().create(versionPath, timestamps.getBytes(), ALL_ACL_LIST,
            CreateMode.PERSISTENT);
        LOG.info("not exists path:" + path + " on zk, create firstly.");
        LOG.info("create timestamp:" + timestamp);
      }

      if (exists) {
        boolean existsVersion = holder.get().exists(versionPath, true) != null;
        Map<String, Long> timestamps = new HashMap<String, Long>();
        if (existsVersion) {
          timestamps.putAll(loadRemoteAllTimeStamp(holder, versionPath, false));
        }
        final Map<String, Long> localTimestamps = new HashMap<String, Long>();
        Iterator<String> keySetIterator = rsFilesMap.keySet().iterator();
        while (keySetIterator.hasNext()) {
          localTimestamps.put(keySetIterator.next(), timestamp);
        }

        LOG.info("upload resources's timestamp:" + localTimestamps
            + ", last timestamp:" + timestamps);

        timestamps.putAll(localTimestamps);
        byte[] timestampsBytes = timestampToString(timestamps).getBytes();
        if (existsVersion) {
          holder.get().setData(versionPath, timestampsBytes, -1);
        } else {
          holder.get().create(versionPath, timestampsBytes, ALL_ACL_LIST,
              CreateMode.PERSISTENT);
        }

        List<String> children = holder.get().getChildren(path, true);

        for (String child : children) {
          File file = rsFilesMap.remove(child);
          if (file != null) {
            holder.get().setData(path + ZK_PATH_SPLITER
                + child, read(file).getBytes(), -1);
            LOG.info("upload resource:" + child + ".xml for updating");
          }
        }
      }

      for (Entry<String, File> entry : rsFilesMap.entrySet()) {
        holder.get().create(path + ZK_PATH_SPLITER + entry.getKey(),
            read(entry.getValue()).getBytes(),
            ALL_ACL_LIST, CreateMode.PERSISTENT);
        LOG.info("upload resource:" + entry.getKey() + ".xml for creating");
      }

      LOG.info("upload finished,cost:"
          + (System.currentTimeMillis() - start) + " ms");

    } catch (Exception e) {
      LOG.error("connect zookeeper occur exception, quorum:" + zkQuorum, e);
      return false;
    } finally {
      holder.close();
    }
    return true;
  }

  private String timestampToString(Map<String, Long> timestamps) {
    return Joiner.on(COMMA).withKeyValueSeparator(COLON)
        .join(timestamps).toString();
  }

  /**
   * drop remote resource.
   *
   * @param nameNodeUri
   * @param zkQuorum
   * @param resources
   * @throws Exception
   */
  public boolean dropResource(String nameNodeUri, String zkQuorum,
      Configuration config, final String... resources) {
    long start = System.currentTimeMillis();
    URI nnUri = null;
    ZooKeeperHolder holder = null;
    try {
      nnUri = new URI(nameNodeUri);
    } catch (URISyntaxException e) {
      LOG.error("namenode uri is illegal:" + nameNodeUri, e);
      return false;
    }

    if (!HDFS.equals(nnUri.getScheme())) {
      LOG.warn("protocol not hdfs,actual:" + nnUri.getScheme());
      return false;
    }

    if (StringUtils.isEmpty(nnUri.getHost())) {
      LOG.warn("host is null");
      return false;
    }

    holder = ZooKeeperHolder.init(zkQuorum, config);
    try {
      if (!holder.connect()) {
        LOG.warn("can't connect zk,zk's quorum:" + zkQuorum);
        return false;
      }
      final Set<String> rsSet = new HashSet<String>();

      final boolean isAll = filter(Arrays.asList(resources), rsSet);

      final String basePath = REMOTE_BASE_PATH + ZK_PATH_SPLITER
          + nnUri.getHost();

      if (holder.get().exists(basePath, true) == null) {
        LOG.info("not existed data on the path:" + basePath);
        return false;
      }
      List<String> childrenShortName = holder.get().getChildren(basePath, true);

      Iterator<String> iterator = Iterators.filter(childrenShortName.iterator(),
          new Predicate<String>() {
          @Override
          public boolean apply(String input) {
            if (!VERSION.equals(input)) {
              return isAll ? isAll : rsSet.contains(input);
            }
            return false;
          }
        });

      String timeStampPath = basePath + ZK_PATH_SPLITER + VERSION;
      Map<String, Long> remoteTimestamp = new HashMap<String, Long>();

      boolean hasDrop = iterator.hasNext();

      if (!hasDrop) {
        LOG.warn("not exists resources:" + rsSet);
        return false;
      }
      if (!isAll) {
        remoteTimestamp.putAll(loadRemoteAllTimeStamp(holder, timeStampPath));
      }

      while (hasDrop) {
        String node = iterator.next();
        holder.get().delete(basePath + ZK_PATH_SPLITER + node, -1);
        remoteTimestamp.remove(node);
        hasDrop = iterator.hasNext();
        LOG.info("delete resource:" + node);
      }

      if (isAll) {
        holder.get().delete(timeStampPath, -1);
        holder.get().delete(basePath, -1);
        LOG.info("delete base path:" + basePath);
      } else {
        holder.get().setData(timeStampPath,
            timestampToString(remoteTimestamp).getBytes(), -1);
        LOG.info("update timestamp:" + remoteTimestamp);
      }

    } catch (Exception e) {
      LOG.error("connect zookeeper occur exception,quorum:" + zkQuorum, e);
      return false;
    } finally {
      holder.close();
    }
    LOG.info("drop resource finished,cost:"
        + (System.currentTimeMillis() - start) + " ms");
    return true;
  }

  private static boolean filter(List<String> resources,
      Set<String> resourceSet) {
    if (CollectionUtils.isEmpty(resources)) {
      resourceSet.add("core-site");
      resourceSet.add("hdfs-site");
      return false;
    }

    for (String resource : resources) {
      if (resource.equals("*") || resource.equalsIgnoreCase("ALL")) {
        return true;
      }
      resource = resource.toLowerCase();
      int idx = resource.indexOf(XML_SUFFIX);
      if (idx != -1) {
        resource = resource.substring(0, idx);
      }
      resourceSet.add(resource);
    }

    return false;
  }

  private Map<String, InputStream> loadLocalResource(
      Collection<File> localConfigFiles) {
    Map<String, InputStream> inputStreams =
        new LinkedHashMap<String, InputStream>();
    for (File localFile : localConfigFiles) {
      String xml = read(localFile);
      if (StringUtils.isNotBlank(xml)) {
        inputStreams.put(localFile.getName(),
            new ByteArrayInputStream(xml.getBytes()));
        LOG.info("fetch local resource:" + localFile.getName());
      }
    }
    return inputStreams;
  }

  private Map<String, InputStream> loadResource(ZooKeeperHolder holder,
      String authority, File localDir, boolean isAll,
      final Set<String> resources, boolean ignoreLocal) {
    Map<String, InputStream> loadResources =
        new LinkedHashMap<String, InputStream>();

    String remotePath = REMOTE_BASE_PATH + ZK_PATH_SPLITER + authority;

    Map<String, Long> localTimeStamp = new HashMap<String, Long>();
    Set<String> totalResources = new HashSet<String>();
    if (!ignoreLocal) {
      localTimeStamp.putAll(loadAllLocalTimeStamp(new File(localDir, VERSION)));
    }
    Map<String, Long> remoteAllTimeStamp =
        loadRemoteAllTimeStamp(holder, remotePath + ZK_PATH_SPLITER + VERSION);

    Map<String, Long> remoteSpecialTimeStamp;

    if (!isAll && MapUtils.isNotEmpty(remoteAllTimeStamp)) {
      remoteSpecialTimeStamp = Maps.filterKeys(remoteAllTimeStamp,
          new Predicate<String>() {
          @Override
          public boolean apply(String input) {
            return resources.contains(input.toLowerCase());
          }
        });

      if (remoteSpecialTimeStamp.size() != resources.size()) {
        Set<String> missing = new HashSet<String>(resources);
        missing.removeAll(remoteSpecialTimeStamp.keySet());
        LOG.warn("zk resouces timestamp is defective,missing:" + missing);
      }
    } else {
      remoteSpecialTimeStamp = remoteAllTimeStamp;
    }

    if (MapUtils.isNotEmpty(remoteSpecialTimeStamp)) {
      totalResources.addAll(remoteSpecialTimeStamp.keySet());
    }

    if (MapUtils.isNotEmpty(localTimeStamp)) {
      Set<String> localSpecialResources = localTimeStamp.keySet();
      if (!isAll) {
        localSpecialResources.retainAll(resources);
      }
      totalResources.addAll(localSpecialResources);
    }
    Set<String> needLoadLocalResources = new HashSet<String>(totalResources);

    // compare timestamp
    Map<String, Long> needFetchRemoteTimestamp =
        getLocalMissOrLowerStamp(remoteSpecialTimeStamp, localTimeStamp);

    Map<String, byte[]> needStoreResources =
        new LinkedHashMap<String, byte[]>();

    byte[] data = null;
    if (MapUtils.isNotEmpty(needFetchRemoteTimestamp)) {
      needLoadLocalResources.removeAll(needFetchRemoteTimestamp.keySet());

      for (String resource : needFetchRemoteTimestamp.keySet()) {
        data = downloadResource(holder,
            remotePath + ZK_PATH_SPLITER + resource);
        if (data != null) {
          LOG.info("fetch remote resource:" + resource);
          ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
          loadResources.put("remote-" + resource, inputStream);
          needStoreResources.put(resource, data);
        }
      }
    }

    if (CollectionUtils.isNotEmpty(needLoadLocalResources)) {
      List<File> loadFiles = new ArrayList<File>();
      for (String resource : needLoadLocalResources) {
        File localFile = new File(localDir + File.separator
            + resource + XML_SUFFIX);

        if (!localFile.exists()) {
          if (remoteAllTimeStamp.containsKey(resource)) {
            data = downloadResource(holder,
                remotePath + ZK_PATH_SPLITER + resource);
            if (data != null) {
              ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
              loadResources.put("remote-" + resource, inputStream);
              needStoreResources.put(resource, data);
              localTimeStamp.put(resource, remoteAllTimeStamp.get(resource));
              LOG.info("fetch remote resource:" + resource
                  + " cause of not exits on local");
            }
          }
          localTimeStamp.remove(resource);
        } else {
          loadFiles.add(localFile);
        }
      }
      loadResources.putAll(loadLocalResource(loadFiles));
    }

    if (MapUtils.isNotEmpty(needStoreResources)) {
      localTimeStamp.putAll(remoteSpecialTimeStamp);
      storeLocalResource(localDir, needStoreResources, localTimeStamp);
    }
    return loadResources;
  }

  public boolean downloadResource(Configuration config, String... specifiedRs) {
    String nameNodeUri = config.get(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY,
        CommonConfigurationKeys.FS_DEFAULT_NAME_DEFAULT);
    String quorum = config.get(CONFIG_ZK_QUORUM_KEY);
    String dir = config.get(CONFIG_DIR_KEY);
    if (nameNodeUri == null || quorum == null || dir == null) {
      LOG.warn("config is defective,"
          + CommonConfigurationKeys.FS_DEFAULT_NAME_KEY + ":" + nameNodeUri
          + "," + CONFIG_ZK_QUORUM_KEY + ":" + quorum + "," + CONFIG_DIR_KEY
          + ":" + dir);
      return false;
    }

    return downloadResource(nameNodeUri, quorum, dir, specifiedRs, config);
  }

  private boolean downloadResource(String nameNodeUri, String quorum,
      String dir, String[] specifiedRs, Configuration config) {
    long start = System.currentTimeMillis();
    URI nnUri = null;
    ZooKeeperHolder holder = null;
    try {
      nnUri = new URI(nameNodeUri);
    } catch (URISyntaxException e) {
      LOG.error("namenode uri is illegal:" + nameNodeUri, e);
      return false;
    }

    if (!HDFS.equals(nnUri.getScheme())) {
      LOG.warn("protocol not hdfs,actual:" + nnUri.getScheme());
      return false;
    }

    if (StringUtils.isEmpty(nnUri.getHost())) {
      LOG.warn("host is null");
      return false;
    }

    holder = ZooKeeperHolder.init(quorum, config);
    try {
      if (!holder.connect()) {
        LOG.warn("can't connect zk,zk's quorum:" + quorum);
        return false;
      }

      File dest = new File(dir);
      if (!checkLocalPath(dest)) {
        LOG.warn("dest not exists:" + dir);
        return false;
      }

      final Set<String> rsSet = new HashSet<String>();

      final boolean isAll = filter(Arrays.asList(specifiedRs), rsSet);
      final String basePath = REMOTE_BASE_PATH + ZK_PATH_SPLITER
          + nnUri.getHost();

      if (holder.get().exists(basePath, true) == null) {
        LOG.info("not existed data on the path:" + basePath);
        return false;
      }
      List<String> childrenShortName = holder.get().getChildren(basePath, true);

      Iterator<String> iterator = Iterators.filter(childrenShortName.iterator(),
          new Predicate<String>() {
          @Override
          public boolean apply(String input) {
            if (!VERSION.equals(input)) {
              return isAll ? isAll : rsSet.contains(input);
            }
            return false;
          }
        });

      boolean hasNext = iterator.hasNext();

      if (!hasNext) {
        LOG.warn("not exists resources:" + rsSet);
        return false;
      }
      String timeStampPath = basePath + ZK_PATH_SPLITER + VERSION;
      Map<String, byte[]> storeResources = new HashMap<String, byte[]>();
      Map<String, Long> remoteTimestamp = loadRemoteAllTimeStamp(holder,
          timeStampPath);
      while (hasNext) {
        String node = iterator.next();
        byte[] data = holder.get().getData(basePath + ZK_PATH_SPLITER + node,
            true, null);
        storeResources.put(node, data);
        hasNext = iterator.hasNext();
      }
      storeLocalResource(dest, storeResources, remoteTimestamp);
      LOG.info("download resource finished,cost:"
          + (System.currentTimeMillis() - start) + " ms");
    } catch (Exception e) {
      LOG.error(" occur exception, zk quorum:" + quorum, e);
      return false;
    } finally {
      holder.close();
    }
    return true;
  }

  public boolean dumpResource(Configuration config, String... specifiedRs) {
    String nameNodeUri = config.get(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY,
        CommonConfigurationKeys.FS_DEFAULT_NAME_DEFAULT);
    String quorum = config.get(CONFIG_ZK_QUORUM_KEY);
    String dir = config.get(CONFIG_DIR_KEY);
    if (nameNodeUri == null || quorum == null || dir == null) {
      LOG.warn("config is defective,"
          + CommonConfigurationKeys.FS_DEFAULT_NAME_KEY + ":" + nameNodeUri
          + "," + CONFIG_ZK_QUORUM_KEY + ":" + quorum + "," + CONFIG_DIR_KEY
          + ":" + dir);
      return false;
    }

    return dumpResource(nameNodeUri, quorum, dir, config, specifiedRs);
  }

  public boolean dumpResource(String nameNodeUri, String quorum, String dir,
      Configuration config, String... specifiedRs) {
    long start = System.currentTimeMillis();
    URI nnUri = null;
    ZooKeeperHolder holder = null;
    try {
      nnUri = new URI(nameNodeUri);
    } catch (URISyntaxException e) {
      LOG.error("namenode uri is illegal:" + nameNodeUri, e);
      return false;
    }

    if (!HDFS.equals(nnUri.getScheme())) {
      LOG.warn("protocol not hdfs,actual:" + nnUri.getScheme());
      return false;
    }

    if (StringUtils.isEmpty(nnUri.getHost())) {
      LOG.warn("host is null");
      return false;
    }

    holder = ZooKeeperHolder.init(quorum, config);
    try {
      if (!holder.connect()) {
        LOG.warn("can't connect zk,zk's quorum:" + quorum);
        return false;
      }

      File dest = new File(dir);
      if (!checkLocalPath(dest)) {
        LOG.warn("dest not exists:" + dir);
        return false;
      }

      final Set<String> rsSet = new HashSet<String>();

      final boolean isAll = filter(Arrays.asList(specifiedRs), rsSet);
      final String basePath = REMOTE_BASE_PATH + ZK_PATH_SPLITER
          + nnUri.getHost();

      if (holder.get().exists(basePath, true) == null) {
        LOG.info("not existed data on the path:" + basePath);
        return false;
      }
      List<String> childrenShortName = holder.get().getChildren(basePath, true);

      Iterator<String> iterator = Iterators.filter(childrenShortName.iterator(),
          new Predicate<String>() {
          @Override
          public boolean apply(String input) {
            if (!VERSION.equals(input)) {
              return isAll ? isAll : rsSet.contains(input);
            }
            return false;
          }
        });

      boolean hasNext = iterator.hasNext();

      if (!hasNext) {
        LOG.warn("not exists resources:" + rsSet);
        return false;
      }
      Configuration conf = new Configuration(false);
      while (hasNext) {
        String node = iterator.next();

        byte[] data = holder.get().getData(basePath + ZK_PATH_SPLITER + node,
            true, null);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
        conf.addResource(inputStream, "remote-" + node);
        hasNext = iterator.hasNext();
        LOG.info("dump resource:" + node);
      }
      dump(nnUri.getHost(), dest, conf);
      LOG.info("dump resource finished,cost:"
          + (System.currentTimeMillis() - start) + " ms");
    } catch (Exception e) {
      LOG.error("occur exception, zk quorum:" + quorum, e);
      return false;
    } finally {
      holder.close();
    }
    return true;
  }

  private void dump(String nn, File dest, Configuration conf) {
    FileOutputStream out = null;
    try {
      out = new FileOutputStream(new File(dest, nn + "-dump.xml"));
      conf.writeXml(out);
    } catch (Exception e) {
      LOG.error("dump conf error,dest:" + dest.getPath(), e);
    } finally {
      IOUtils.closeStream(out);
    }
  }

  byte[] downloadResource(ZooKeeperHolder holder, String remoteDir) {
    try {
      byte[] resourceData = holder.get().getData(remoteDir, null, null);
      if (resourceData == null) {
        LOG.warn("not found remote resource data from path:" + remoteDir);
      }
      return resourceData;
    } catch (KeeperException e) {
      LOG.error("communicate with zk got exception, major path: "
          + remoteDir, e);
    } catch (InterruptedException e) {
      LOG.error("communicate with zk got exception, major path: "
          + remoteDir, e);
    }
    return null;
  }

  private void storeLocalResource(File localPath, Map<String,
      byte[]> needStoreResources, Map<String, Long> localTimeStamp) {
    OutputStream output = null;
    File storeFile;
    for (Entry<String, byte[]> entry : needStoreResources.entrySet()) {
      storeFile = new File(localPath, entry.getKey() + XML_SUFFIX);
      try {
        if (storeFile.createNewFile()) {
          trySetXWRPermission(storeFile);
        }
        output = new FileOutputStream(storeFile);
        output.write(entry.getValue());
        output.flush();
        LOG.info("store resource:" + storeFile.getName());
      } catch (IOException e) {
        LOG.error("store local resource file failed,parent path:" + localPath
            + ",resource:" + storeFile.getName(), e);
        return;
      } finally {
        IOUtils.closeStream(output);
      }
    }

    MapJoiner joiner = Joiner.on(COMMA).withKeyValueSeparator(COLON);

    storeFile = new File(localPath + File.separator + VERSION);
    try {
      if (storeFile.createNewFile()) {
        trySetXWRPermission(storeFile);
      }
      output = new FileOutputStream(storeFile);
      output.write(joiner.appendTo(new StringBuilder(),
          localTimeStamp).toString().getBytes());
      output.flush();
      LOG.info("store timestamp:" + storeFile.getName()
          + ",timestamp:" + localTimeStamp);
    } catch (IOException e) {
      LOG.error("store local timestamp file failed,parent path:" + localPath
          + ",resource:" + storeFile.getName(), e);
    } finally {
      IOUtils.closeStream(output);
    }
  }

  private Map<String, Long> getLocalMissOrLowerStamp(
      Map<String, Long> remoteSpecialTimeStamp,
      Map<String, Long> localTimeStamp) {
    if (MapUtils.isEmpty(localTimeStamp)) {
      return remoteSpecialTimeStamp;
    }

    Map<String, Long> localMissStamp = new HashMap<String, Long>();

    for (Entry<String, Long> entry : remoteSpecialTimeStamp.entrySet()) {
      Long timestamp = localTimeStamp.get(entry.getKey());
      if (timestamp == null || timestamp.compareTo(entry.getValue()) < 0) {
        localMissStamp.put(entry.getKey(), entry.getValue());
      }
    }

    return localMissStamp;
  }

  private Map<String, Long> loadRemoteAllTimeStamp(ZooKeeperHolder holder,
      String versionPath) {
    return loadRemoteAllTimeStamp(holder, versionPath, false);
  }

  Map<String, Long> loadRemoteAllTimeStamp(ZooKeeperHolder holder,
      String versionPath, boolean deleteIfExists) {
    Map<String, Long> versionMap = new HashMap<String, Long>();

    try {
      if (holder.get().exists(versionPath, true) != null) {
        byte[] bytes = holder.get().getData(versionPath, false, null);
        if (bytes != null) {
          String timestamps = new String(bytes);
          versionMap.putAll(buildResourceTimeStampPair(timestamps));
          if (deleteIfExists) {
            holder.get().delete(versionPath, -1);
          }
        }
      }
    } catch (Exception e) {
      LOG.error("connect zk faild.", e);
    }

    return versionMap;
  }

  private Map<String, Long> loadAllLocalTimeStamp(File versionFile) {
    Map<String, Long> versionMap = new HashMap<String, Long>();
    if (!versionFile.exists()) {
      return versionMap;
    }
    String versionStr = read(versionFile);
    versionMap.putAll(buildResourceTimeStampPair(versionStr));
    return versionMap;
  }

  private String read(File versionFile) {
    StringWriter writer = new StringWriter();
    char[] cbuf = new char[512];
    int idx = -1;
    Reader reader = null;
    try {
      reader = new InputStreamReader(new FileInputStream(versionFile));
      while ((idx = reader.read(cbuf)) != -1) {
        writer.write(cbuf, 0, idx);
      }
    } catch (IOException e) {
      LOG.error("unable load local config file version,file:"
          + versionFile.getName(), e);
    } finally {
      IOUtils.closeStream(reader);
    }

    String versionStr = writer.getBuffer().toString();
    return versionStr;
  }

  private Map<String, Long> buildResourceTimeStampPair(String versionStr) {
    Map<String, Long> configVersionMap = new HashMap<String, Long>();
    for (Iterator<String> iterator = Splitter.on(COMMA).trimResults()
        .split(versionStr).iterator(); iterator.hasNext();) {
      String[] versionPair = iterator.next().split(COLON);
      if (versionPair.length == 2) {
        configVersionMap.put(versionPair[0], Long.valueOf(versionPair[1]));
      }
    }
    return configVersionMap;
  }

  private boolean checkLocalPath(File localConfigDir) {
    return checkLocalPath(localConfigDir, true);
  }

  private boolean checkLocalPath(File localConfigDir, boolean createIfAbsent) {
    try {
      if (!localConfigDir.exists()) {
        if (createIfAbsent && localConfigDir.mkdirs()) {
          trySetXWRPermission(localConfigDir);
          return true;
        }
        return false;
      }
      return localConfigDir.isDirectory() && localConfigDir.canRead()
          && localConfigDir.canWrite();
    } catch (Exception e) {
      LOG.error("check local config dir faild,dir:" + localConfigDir, e);
      return false;
    }
  }

  private boolean trySetXWRPermission(File file) {
    if(file == null || !file.exists()){
      return false;
    }
    try {
      file.setExecutable(true, false);
      file.setReadable(true, false);
      file.setWritable(true, false);
    } catch(Exception e) {
      LOG.error("set xwr permission failed", e);
      return false;
    }
    return true;
  }

  private boolean existsAnyResources(File localConfigDir) {
    if (checkLocalPath(localConfigDir)) {
      return ArrayUtils.isNotEmpty(localConfigDir.listFiles(new FileFilter() {
        @Override
        public boolean accept(File file) {
          return file.isFile() && file.getName().endsWith(XML_SUFFIX);
        }
      }));
    }
    return false;
  }

  private boolean existsSpecResources(File localConfigDir,
      final Set<String> specialRs, boolean allMatched) {
    if (checkLocalPath(localConfigDir)) {
      File[] files = localConfigDir.listFiles(new FileFilter() {
        @Override
        public boolean accept(File file) {
          int idx = file.getName().indexOf(XML_SUFFIX);
          if (file.isFile() && (idx != -1)) {
            return specialRs.contains(file.getName().substring(0, idx)
                .toLowerCase());
          }
          return false;
        }

      });
      return allMatched ? (files != null && files.length == specialRs.size())
          : (files != null && files.length > 0);
    }
    return false;
  }

  private Map<String, File> getLocalFile(File localConfigDir,
      final Collection<String> resources, final boolean isAll) {
    if (!localConfigDir.exists() || !localConfigDir.isDirectory()) {
      return Collections.emptyMap();
    }

    final Map<String, File> resourceMap = new HashMap<String, File>();

    File[] configFiles = localConfigDir.listFiles(new FileFilter() {
      @Override
      public boolean accept(File file) {
        String fileName = file.getName().toLowerCase();
        int idx = fileName.indexOf(XML_SUFFIX);
        if (file.isFile() && (idx != -1)) {
          if (isAll) {
            return true;
          }
          fileName = fileName.substring(0, idx);

          for (String resource : resources) {
            if (fileName.equals(resource)) {
              return true;
            }
          }
          return false;
        }
        return false;
      }
    });

    if (configFiles != null) {
      for (File configFile : configFiles) {
        String fileName = configFile.getName();
        fileName = fileName.substring(0, fileName.indexOf(XML_SUFFIX));
        resourceMap.put(fileName, configFile);
      }
    }

    return resourceMap;
  }

  /**
   * thread safe mode,reconnect if session expired.
   */
  public static final class ZooKeeperHolder {
    private ZooKeeperHolder(String quorum, Configuration configuration) {
      this.quorum = quorum;
      this.sessionTimeout =
          configuration.getInt("config.zk.session.timeout",
          DEFAULT_ZK_SESSION_TIMEOUT_IN_MILLIS);
      this.maxRetryTime =
          configuration.getInt("config.zk.connect.max.retry.times", 16);
    }

    private int maxRetryTime = 16;

    private long waitTime = 500L;

    private volatile ZooKeeper zooKeeper;

    private String quorum;

    private int sessionTimeout;

    private Object mutex = new Object();

    private volatile boolean initialized = false;

    private volatile boolean isTimeout = false;

    public static ZooKeeperHolder init(String quorum, Configuration conf) {
      ZooKeeperHolder holder = new ZooKeeperHolder(quorum, conf);
      return holder;
    }

    private void build() {
      try {
        this.zooKeeper = new ZooKeeper(quorum, sessionTimeout,
            this.new DummyWatcher());
      } catch (IOException e) {
        LOG.error("connect zk error.", e);
        this.initialized = true;
      }
      isTimeout = false;
    }

    public boolean connect() {
      return get() != null && get().getState().isConnected();
    }

    public ZooKeeper get() {
      int count = 0;
      if (!initialized) {
        build();
      }
      long waitT = this.waitTime;
      if (zooKeeper != null) {
        while (!initialized || (isTimeout && initialized)) {
          long time = System.currentTimeMillis();
          synchronized (mutex) {
            try {
              mutex.wait(Math.min(waitT, 30000L));
            } catch (InterruptedException e) {
              LOG.warn("get zookeeper is interrupted.", e);
              initialized = true;
              isTimeout = false;
            }
          }
          long actTime = System.currentTimeMillis() - time;
          if (LOG.isDebugEnabled()) {
            LOG.debug("connect zk  cost  " + actTime
                + "ms, and wait count:" + count);
          }
          waitT += actTime;

          if (++count >= maxRetryTime) {
            LOG.warn("get zookeeper over time,retry:" + this.maxRetryTime);
            initialized = true;
            break;
          }
        }
      }

      return zooKeeper;
    }

    public void close() {
      if (zooKeeper != null) {
        try {
          get().close();
        } catch (Exception e) {
          LOG.error("close zookeeper failed.", e);
        }
      }
      initialized = false;
    }

    /**
     * Dummy Watcher.
     */
    public class DummyWatcher implements Watcher {
      @Override
      public void process(WatchedEvent event) {
        if (KeeperState.SyncConnected == event.getState()) {
          initialized = true;
          synchronized (mutex) {
            mutex.notify();
          }
        }

        if (KeeperState.Expired == event.getState()) {
          isTimeout = true;
          close();
          build();
          synchronized (mutex) {
            mutex.notify();
          }
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug("zk event:" + event);
        }
      }
    }

    public void setExtra(int maxRetry, long waitT) {
      this.maxRetryTime = maxRetry;
      this.waitTime = waitT;
    }
  }

  public static void main(String[] args) throws Exception {

    Tool tool = new DistributedConfigTool();
    int res = ToolRunner.run(tool, args);
    System.exit(res);
  }

  /**
   * Mode.
   */
  public enum LoadMode {
    /**
     * load if exists local resoures,not will load from remote and storage
     * to local.
     */
    LOCAL,
    /**
     * only load remote resources.
     */
    REMOTE,
    /**
     * load latest from remote ,else from local.
     */
    MIX;
  }

  /**
   * File Lock.
   */
  public class LocalFileLock {

    private RandomAccessFile raf;
    private File lockFile;
    private boolean shared = true;
    private FileLock fileLock;
    private boolean locked = false;
    private boolean supported = true;
    private boolean initialized = false;

    LocalFileLock(File lockFile, boolean shared) {
      init(lockFile, shared);
    }

    private void init(File lockF, boolean share) {
      this.lockFile = lockF;
      if (supported) {
        if (!lockFile.exists()) {
          try {
            if(lockFile.createNewFile()){
              trySetXWRPermission(lockFile);
            }else{
              supported = false;
            }
          } catch (IOException e) {
            LOG.error("create lock file error,file name:"
                + lockFile.getName(), e);
            supported = false;
          }
        }
      }

      if (supported) {
        try {
          this.raf = new RandomAccessFile(lockFile, "rws");
        } catch (FileNotFoundException e) {
          LOG.error("not found file,file name:" + lockFile.getName(), e);
          supported = false;
        }
      }
      this.shared = share;
      this.locked = false;
      this.initialized = true;
      LOG.debug("support file lock:" + supported);
    }

    public void lock() {
      if (supported) {
        int times = 0;
        while (!locked && times++ < 5) {
          try {
            fileLock = raf.getChannel().lock(0, Long.MAX_VALUE, shared);
          } catch (OverlappingFileLockException e) {
            // multiple thread lock conflict
            ThreadUtil.sleepAtLeastIgnoreInterrupts(500L);
            locked = false;
            continue;
          } catch (IOException e) {
            LOG.error(" lock file error,file name:" + lockFile.getName(), e);
          }
          locked = true;
        }
      }

      supported = locked;
    }

    public void unlock() {
      if (fileLock != null) {
        try {
          fileLock.release();
        } catch (IOException e) {
          LOG.error(" lock file error,file name:" + lockFile.getName(), e);
        }
      }
      close();
    }

    private void close() {
      IOUtils.closeStream(raf);
      raf = null;
      locked = false;
      initialized = false;
      supported = true;
    }

    public void upgradeLock() {
      opLock(true);
    }

    public void demoteLock() {
      opLock(false);
    }

    private void opLock(boolean upgrade) {
      if (!supported) {
        return;
      }
      boolean share = !upgrade;
      if (fileLock == null) {
        this.shared = share;
      }

      if (locked) {
        // not need op
        if (this.shared == share) {
          return;
        }
        unlock();
      }
      if (!initialized) {
        init(lockFile, shared);
      }
      lock();
    }
  }

  /**
   * Operation.
   */
  public enum OperationEnum {
    UPLOAD, DOWNLOAD, DROP, DUMP;
  }

  /**
   * Tool.
   */
  public static class DistributedConfigTool implements Tool {

    private Configuration conf;

    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
    }

    @Override
    public Configuration getConf() {
      return this.conf;
    }

    @Override
    public int run(String[] args) throws Exception {
      DistributedConfigHelper helper = DistributedConfigHelper.get();
      boolean needHelp = false;
      OperationEnum op = null;
      if (args == null || args.length < 1 || !args[0].startsWith("-")) {
        needHelp = true;
      }

      boolean success = false;
      if (!needHelp) {
        op = OperationEnum.valueOf(args[0].substring(1).toUpperCase());
        if (op != null) {
          switch (op) {
          case UPLOAD:
            if (args.length < 3) {
              needHelp = true;
            }
            break;
          case DROP:
            if (args.length < 2) {
              needHelp = true;
            }
            break;
          case DUMP:
            if (args.length < 3) {
              needHelp = true;
            }
            break;
          case DOWNLOAD:
            if (args.length < 3) {
              needHelp = true;
            }
            break;
          default:
            needHelp = true;
            break;
          }
        }
      }

      if (!needHelp && op != null) {
        String[] specifiedRs = new String[] {"*"};

        switch (op) {
        case UPLOAD:
          if (args.length > 3) {
            specifiedRs = new String[args.length - 3];
            System.arraycopy(args, 3, specifiedRs, 0, specifiedRs.length);
          }
          conf.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, args[1]);
          conf.set(CONFIG_DIR_KEY, args[2]);
          success = helper.uploadResource(conf, specifiedRs);
          break;
        case DROP:
          if (args.length > 2) {
            specifiedRs = new String[args.length - 2];
            System.arraycopy(args, 2, specifiedRs, 0, specifiedRs.length);
          }
          conf.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, args[1]);
          success = helper.dropResource(conf, specifiedRs);
          break;
        case DUMP:
          if (args.length > 3) {
            specifiedRs = new String[args.length - 3];
            System.arraycopy(args, 3, specifiedRs, 0, specifiedRs.length);
          }
          conf.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, args[1]);
          conf.set(CONFIG_DIR_KEY, args[2]);
          success = helper.dumpResource(conf, specifiedRs);
          break;
        case DOWNLOAD:
          if (args.length > 3) {
            specifiedRs = new String[args.length - 3];
            System.arraycopy(args, 3, specifiedRs, 0, specifiedRs.length);
          }
          conf.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, args[1]);
          conf.set(CONFIG_DIR_KEY, args[2]);
          success = helper.downloadResource(conf, specifiedRs);
          break;
        default:
          break;
        }
        System.out.println(op.toString().toLowerCase()
            + (success ? " successfully" : " faild"));
      }

      if (needHelp) {
        if (op != null) {
          switch (op) {
          case UPLOAD:
            System.out.println("usage:[-upload <fs> <confdir> <resource>...]");
            break;
          case DROP:
            System.out.println("usage:[-drop <fs> <resource>...]");
            break;
          case DUMP:
            System.out.println("usage:[-dump <fs> <dest> <resource>...]");
            break;
          case DOWNLOAD:
            System.out.println("usage:[-download <fs> <dest> <resource>...]");
            break;
          default:
            break;
          }
        } else {
          System.out.println("usage:[-upload|-download|-drop|-dump]");
        }
      }
      return -1;
    }
  }
}