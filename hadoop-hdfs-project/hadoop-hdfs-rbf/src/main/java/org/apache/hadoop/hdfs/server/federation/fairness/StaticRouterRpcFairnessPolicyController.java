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

package org.apache.hadoop.hdfs.server.federation.fairness;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.router.FederationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.Semaphore;

import static org.apache.hadoop.hdfs.server.federation.fairness.RouterRpcFairnessConstants.CONCURRENT_NS;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_HANDLER_COUNT_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_FAIR_HANDLER_COUNT_KEY_PREFIX;

/**
 * Static fairness policy extending @AbstractRouterRpcFairnessPolicyController
 * and fetching handlers from configuration for all available name services.
 * The handlers count will not change for this controller.
 */
public class StaticRouterRpcFairnessPolicyController extends
    AbstractRouterRpcFairnessPolicyController {

  private static final Logger LOG =
      LoggerFactory.getLogger(StaticRouterRpcFairnessPolicyController.class);

  Set<String> allConfiguredNS = new HashSet<>();
  RouterRpcFairnessPolicyUpdateService updateService;

  public StaticRouterRpcFairnessPolicyController(Configuration conf) {
    init(conf);
  }

  public void init(Configuration conf)
      throws IllegalArgumentException {
    super.init(conf);

    // Get all name services configured
    allConfiguredNS = FederationUtil.getAllConfiguredNS(getConf());
    assignHandlers(allConfiguredNS);

    // Start update service
    updateService = new RouterRpcFairnessPolicyUpdateService(this);
    updateService.init(conf);
    updateService.start();
  }

  private void assignHandlers(Set<String> configuredNS) {
    Map<String, Semaphore> permits = new HashMap<>();
    // Total handlers configured to process all incoming Rpc.
    int handlerCount = getConf().getInt(
        DFS_ROUTER_HANDLER_COUNT_KEY,
        DFS_ROUTER_HANDLER_COUNT_DEFAULT);

    LOG.info("Handlers available for fairness assignment {} ", handlerCount);


    // Set to hold name services that are not
    // configured with dedicated handlers.
    Set<String> unassignedNS = new HashSet<>();

    // Insert the concurrent nameservice into the set to process together
    configuredNS.add(CONCURRENT_NS);
    for (String nsId : configuredNS) {
      int dedicatedHandlers =
          getConf().getInt(DFS_ROUTER_FAIR_HANDLER_COUNT_KEY_PREFIX + nsId, 0);
      LOG.info("Dedicated handlers {} for ns {} ", dedicatedHandlers, nsId);
      if (dedicatedHandlers > 0) {
        handlerCount -= dedicatedHandlers;
        // Total handlers should not be less than sum of dedicated
        // handlers.
        validateCount(nsId, handlerCount, 0);
        permits.put(nsId, new Semaphore(dedicatedHandlers));
        logAssignment(nsId, dedicatedHandlers);
      } else {
        unassignedNS.add(nsId);
      }
    }

    // Assign remaining handlers equally to remaining name services and
    // general pool if applicable.
    if (!unassignedNS.isEmpty()) {
      LOG.info("Unassigned ns {}", unassignedNS.toString());
      int handlersPerNS = handlerCount / unassignedNS.size();
      LOG.info("Handlers available per ns {}", handlersPerNS);
      for (String nsId : unassignedNS) {
        // Each NS should have at least one handler assigned.
        validateCount(nsId, handlersPerNS, 1);
        permits.put(nsId, new Semaphore(handlersPerNS));
        logAssignment(nsId, handlersPerNS);
      }
    }

    // Assign remaining handlers if any to fan out calls.
    int leftOverHandlers = handlerCount % unassignedNS.size();
    int existingPermits = permits.get(CONCURRENT_NS).availablePermits();
    if (leftOverHandlers > 0) {
      LOG.info("Assigned extra {} handlers to commons pool", leftOverHandlers);
      permits.put(CONCURRENT_NS,
          new Semaphore(existingPermits + leftOverHandlers));
    }
    LOG.info("Final permit allocation for concurrent ns: {}",
        permits.get(CONCURRENT_NS).availablePermits());
    setPermits(permits);
  }

  private static void logAssignment(String nsId, int count) {
    LOG.info("Assigned {} handlers to nsId {} ",
        count, nsId);
  }

  private static void validateCount(String nsId, int handlers, int min) throws
      IllegalArgumentException {
    if (handlers < min) {
      String msg =
          "Available handlers " + handlers +
          " lower than min " + min +
          " for nsId " + nsId;
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }
  }

  public void update() {
    Set<String> tmpConfiguredNS = FederationUtil.getAllConfiguredNS(getConf());
    if (!isConfiguredNSChanged(tmpConfiguredNS)) {
      return;
    }
    assignHandlers(tmpConfiguredNS);
    allConfiguredNS = tmpConfiguredNS;
  }

  private boolean isConfiguredNSChanged(Set<String> configuredNS) {
    Iterator<String> it = configuredNS.iterator();
    while(it.hasNext()) {
      String ns = it.next();
      if (!allConfiguredNS.contains(ns)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void shutdown() {
    updateService.stop();
    super.shutdown();
  }
}
