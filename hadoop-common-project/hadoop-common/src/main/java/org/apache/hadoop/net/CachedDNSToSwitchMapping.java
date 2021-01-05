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
package org.apache.hadoop.net;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;

/**
 * A cached implementation of DNSToSwitchMapping that takes an
 * raw DNSToSwitchMapping and stores the resolved network location in 
 * a cache. The following calls to a resolved network location
 * will get its location from the cache. 
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class CachedDNSToSwitchMapping extends AbstractDNSToSwitchMapping {
  private static final Log LOG = LogFactory.getLog(CachedDNSToSwitchMapping.class);

  private Map<String, String> cache = new ConcurrentHashMap<String, String>();

  /**
   * The uncached mapping
   */
  protected final DNSToSwitchMapping rawMapping;

  /**
  * The script to get all rack resolution items upon initiating.
  */
  private final String getAllRackResolveItemsScript;

  /**
   * cache a raw DNS mapping
   * @param rawMapping the raw mapping to cache
   */
  public CachedDNSToSwitchMapping(DNSToSwitchMapping rawMapping) {
    this.rawMapping = rawMapping;

    // initialize caches
    Configuration conf = new Configuration();
    conf.addResource("hdfs-site.xml");
    this.getAllRackResolveItemsScript = conf.get(
        ScriptBasedMapping.GET_ALL_ITEMS_SCRIPT_FILENAME_KEY, null);
    if (this.getAllRackResolveItemsScript != null) {
      loadAllRackResolveItems();
    }
  }

  /**
   * Load all rack resolution results.
   */
  private void loadAllRackResolveItems() {
    long startTime = System.currentTimeMillis();

    // Get all rack resolution items, using shell command executor
    List<String> cmdList = new ArrayList<String>();
    cmdList.add(getAllRackResolveItemsScript);

    File dir = null;
    String userDir;
    if ((userDir = System.getProperty("user.dir")) != null) {
      dir = new File(userDir);
    }
    ShellCommandExecutor s = new ShellCommandExecutor(
        cmdList.toArray(new String[cmdList.size()]), dir);
    String results;
    try {
      s.execute();
      results = s.getOutput();
    } catch (Exception e) {
      LOG.warn("Exception running " + s
          + " while loading all rack resolution items.", e);
      return;
    }

    /*
     * rack resolution items' format:
     * 1. one item per line.
     * 2. each item should be presented as: nodeIP rack
     */
    BufferedReader in = new BufferedReader(new InputStreamReader(
        new ByteArrayInputStream(results.getBytes())));
    int number = 0;
    while (true) {
      String line = null;
      try {
        line = in.readLine();
      } catch (IOException e) {
        LOG.warn("Loading all rack resolution items aborted.", e);
        break;
      }

      // meets EOF
      if (line == null) {
        break;
      }

      // skip blank lines
      if (line.trim().isEmpty()) {
        continue;
      }

      // cache a item
      String [] components = line.split("\\s+");
      if (components.length != 2) {
        LOG.warn("Loading all rack resolution results skipped illegal item "
            + line + ",  each item should be presented as: nodeIP rack.");
        continue;
      }
      String node = components[0];
      String rack = components[1];

      // sanity check
      if (!rack.startsWith(Path.SEPARATOR)) {
        LOG.warn("Loading all rack resolution results skipped illegal item "
            + line + ",  each item should be presented as: nodeIP rack.");
        continue;
      }

      // now cache it
      cache.put(node, rack);
      number++;
    }

    long elapsedTime = System.currentTimeMillis() - startTime;
    LOG.info("Successfully loaded " + number
          + " rack resolution items in " + elapsedTime + " ms.");
  }

  /**
   * @param names a list of hostnames to probe for being cached
   * @return the hosts from 'names' that have not been cached previously
   */
  private List<String> getUncachedHosts(List<String> names) {
    // find out all names without cached resolved location
    List<String> unCachedHosts = new ArrayList<String>(names.size());
    for (String name : names) {
      if (cache.get(name) == null) {
        unCachedHosts.add(name);
      } 
    }
    return unCachedHosts;
  }

  /**
   * Caches the resolved host:rack mappings. The two list
   * parameters must be of equal size.
   *
   * @param uncachedHosts a list of hosts that were uncached
   * @param resolvedHosts a list of resolved host entries where the element
   * at index(i) is the resolved value for the entry in uncachedHosts[i]
   */
  private void cacheResolvedHosts(List<String> uncachedHosts, 
      List<String> resolvedHosts) {
    // Cache the result
    if (resolvedHosts != null) {
      for (int i=0; i<uncachedHosts.size(); i++) {
        cache.put(uncachedHosts.get(i), resolvedHosts.get(i));
      }
    }
  }

  /**
   * @param names a list of hostnames to look up (can be be empty)
   * @return the cached resolution of the list of hostnames/addresses.
   *  or null if any of the names are not currently in the cache
   */
  private List<String> getCachedHosts(List<String> names) {
    List<String> result = new ArrayList<String>(names.size());
    // Construct the result
    for (String name : names) {
      String networkLocation = cache.get(name);
      if (networkLocation != null) {
        result.add(networkLocation);
      } else {
        return null;
      }
    }
    return result;
  }

  @Override
  public List<String> resolve(List<String> names) {
    // normalize all input names to be in the form of IP addresses
    long time = System.currentTimeMillis();
    names = NetUtils.normalizeHostNames(names);
    if(LOG.isDebugEnabled()) {
      time = System.currentTimeMillis() - time;
      LOG.debug("normalizeHostNames time: " + time + "ms, hosts: " + String.join(",", names));
    }

    List <String> result = new ArrayList<String>(names.size());
    if (names.isEmpty()) {
      return result;
    }

    List<String> uncachedHosts = getUncachedHosts(names);

    time = System.currentTimeMillis();
    // Resolve the uncached hosts
    List<String> resolvedHosts = rawMapping.resolve(uncachedHosts);
    if(LOG.isDebugEnabled()) {
      time = System.currentTimeMillis() - time;
      LOG.debug("resolve time: " + time + "ms，hosts: " + String.join(",", uncachedHosts));
    }

    //cache them
    cacheResolvedHosts(uncachedHosts, resolvedHosts);
    //now look up the entire list in the cache
    return getCachedHosts(names);
  }

  /**
   * Get the (host x switch) map.
   * @return a copy of the cached map of hosts to rack
   */
  @Override
  public Map<String, String> getSwitchMap() {
    Map<String, String > switchMap = new HashMap<String, String>(cache);
    return switchMap;
  }


  @Override
  public String toString() {
    return "cached switch mapping relaying to " + rawMapping;
  }

  /**
   * Delegate the switch topology query to the raw mapping, via
   * {@link AbstractDNSToSwitchMapping#isMappingSingleSwitch(DNSToSwitchMapping)}
   * @return true iff the raw mapper is considered single-switch.
   */
  @Override
  public boolean isSingleSwitch() {
    return isMappingSingleSwitch(rawMapping);
  }
  
  @Override
  public void reloadCachedMappings() {
    cache.clear();
  }

  @Override
  public void reloadCachedMappings(List<String> names) {
    for (String name : names) {
      cache.remove(name);
    }
  }
}
