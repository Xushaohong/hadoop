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
package org.apache.hadoop.ipc.metrics;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableRatesWithAggregation;
import org.apache.hadoop.metrics2.util.MBeans;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectName;

/**
 * This class is for maintaining RPC method related statistics
 * and publishing them through the metrics interfaces.
 */
@InterfaceAudience.Private
@Metrics(about="Per method RPC metrics", context="rpcdetailed")
public class RpcDetailedMetrics implements RpcDetailedMetricsMBean {

  @Metric MutableRatesWithAggregation rates;
  @Metric MutableRatesWithAggregation deferredRpcRates;

  static final Logger LOG = LoggerFactory.getLogger(RpcDetailedMetrics.class);
  final MetricsRegistry registry;
  final String name;
  // A map of "ThreadID -> currently performing RPC method".
  final ConcurrentHashMap<Long, String> threadRPCMethod;
  ObjectName nnMBeanObjectName;

  RpcDetailedMetrics(int port) {
    name = "RpcDetailedActivityForPort" + port;
    registry = new MetricsRegistry(name)
        .tag("port", "RPC port", String.valueOf(port));
    LOG.debug(registry.info().toString());

    // Do not use @Metric annotation to generate JMX, as
    // @Metric has a default 60s cache period therefore not real-time.
    threadRPCMethod = new ConcurrentHashMap<Long, String>();
    nnMBeanObjectName = MBeans
        .register("NameNode", this.getClass().getSimpleName() + port, this);
  }

  public String name() { return name; }

  public static RpcDetailedMetrics create(int port) {
    RpcDetailedMetrics m = new RpcDetailedMetrics(port);
    return DefaultMetricsSystem.instance().register(m.name, null, m);
  }

  /**
   * Initialize the metrics for JMX with protocol methods
   * @param protocol the protocol class
   */
  public void init(Class<?> protocol) {
    rates.init(protocol);
    deferredRpcRates.init(protocol);
  }

  /**
   * Add an RPC processing time sample
   * @param rpcCallName of the RPC call
   * @param processingTime  the processing time
   */
  //@Override // some instrumentation interface
  public void addProcessingTime(String rpcCallName, long processingTime) {
    rates.add(rpcCallName, processingTime);
  }

  public void addDeferredProcessingTime(String name, long processingTime) {
    deferredRpcRates.add(name, processingTime);
  }

  public void setThreadRPCMethod(String method) {
    Long threadId = Thread.currentThread().getId();
    threadRPCMethod.put(threadId, method);
  }

  public void clearThreadRPCMethod() {
    Long threadId = Thread.currentThread().getId();
    threadRPCMethod.put(threadId, StringUtils.EMPTY);
  }

  @Override
  public String getRPCMethodsThreadCount() {
    LinkedList<String> methods =
        new LinkedList<String>(threadRPCMethod.values());

    // remove EMPTY elements
    methods.removeIf(e -> e.isEmpty());

    // higher frequency RPC method displayed first.
    Comparator<String> cmp = new Comparator<String>() {
      @Override
      public int compare(String o1, String o2) {
        if (Collections.frequency(methods, o1)
            > Collections.frequency(methods, o2)) {
          return -1;
        } else if (Collections.frequency(methods, o1)
            == Collections.frequency(methods, o2)) {
          return 0;
        } else {
          return 1;
        }
      }
    };

    // generate a TreeMap of "RPC method -> corresponding handler count"
    TreeMap<String, Long> methodsThreadCount =
        methods.stream().collect(Collectors.groupingBy(
                                e -> e,
                                () -> new TreeMap<>(cmp),
                                Collectors.counting()));
    try {
      return new ObjectMapper().writeValueAsString(methodsThreadCount);
    } catch (IOException e) {
      LOG.warn("Failed to fetch threads op count", e);
    }
    return null;
  }

  /**
   * Shutdown the instrumentation for the process
   */
  //@Override // some instrumentation interface
  public void shutdown() {
    DefaultMetricsSystem.instance().unregisterSource(name);
    MBeans.unregister(nnMBeanObjectName);
  }
}
