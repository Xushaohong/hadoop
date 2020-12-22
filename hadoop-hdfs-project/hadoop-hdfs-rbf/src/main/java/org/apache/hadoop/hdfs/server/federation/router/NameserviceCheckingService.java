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
package org.apache.hadoop.hdfs.server.federation.router;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.DistributedConfigHelper;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_MONITOR_NAMENODE;

public class NameserviceCheckingService extends PeriodicService {
  private static final Logger LOG =
      LoggerFactory.getLogger(NamenodeHeartbeatService.class);

  private final Router router;
  private final HashSet<String> allNameServices = new HashSet<String>();
  private final HashSet<NamenodeHeartbeatService> serviceSet =
      new HashSet<NamenodeHeartbeatService>();

  public NameserviceCheckingService(Router router) {
    super(NameserviceCheckingService.class.getSimpleName());
    this.router = router;
    Iterator<NamenodeHeartbeatService> it =
        router.getNamenodeHeartbeatServices().iterator();
    while(it.hasNext()) {
      NamenodeHeartbeatService service = it.next();
      String nameserviceId = service.getNameserviceId();
      allNameServices.add(nameserviceId);
    }
  }

  @Override
  protected void periodicInvoke() {
    updateNameService();
  }

  public void updateNameService() {
    Configuration conf = router.getConfig();

    Collection<String> nsIds = DistributedConfigHelper.get()
        .getAllNameServices(conf);
    ArrayList<String> newNamenodes = new ArrayList<>();
    for(String nsId : nsIds) {
      if (!allNameServices.contains(nsId)) {
        Configuration disconfig = DistributedConfigHelper.get().build("hdfs://" + nsId, conf);
        Collection<String> nnIds = disconfig.getTrimmedStringCollection(
            DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX + "." + nsId);
        for (String nnId : nnIds) {
          NamenodeHeartbeatService heartbeatService =
              router.createNamenodeHeartbeatService(nsId, nnId);
          try {
            heartbeatService.serviceInit(conf);
            heartbeatService.serviceStart();
            LOG.info("Create heartbeat service for nsId: {}, nnId: {}", nsId, nnId);
            serviceSet.add(heartbeatService);
            newNamenodes.add(nsId + "." + nnId);
          } catch (Exception e) {
            LOG.error("Create heartbeat service error for nsId: {}, nnId: {}.",
                nsId, nnId, e);
            continue;
          }
        }
        allNameServices.add(nsId);
        LOG.info("Add NameService: {}", nsId);
      }
    }
    // update dfs.federation.router.monitor.namenode
    if (!newNamenodes.isEmpty()) {
      StringBuilder sb = new StringBuilder(
          conf.get(DFS_ROUTER_MONITOR_NAMENODE, ""));
      for (String namenode : newNamenodes) {
        if (sb.length() > 0) {
          sb.append(",");
        }
        sb.append(namenode);
      }
      conf.set(DFS_ROUTER_MONITOR_NAMENODE, sb.toString());
    }
  }

  @Override
  public void serviceStop() throws Exception{
    Iterator<NamenodeHeartbeatService> it = serviceSet.iterator();
    while(it.hasNext()) {
      NamenodeHeartbeatService service = it.next();
      service.serviceStop();
    }
    super.serviceStop();
  }
}
