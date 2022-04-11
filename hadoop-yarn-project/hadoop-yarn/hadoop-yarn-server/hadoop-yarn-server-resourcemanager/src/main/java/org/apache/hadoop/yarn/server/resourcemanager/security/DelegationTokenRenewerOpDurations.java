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
package org.apache.hadoop.yarn.server.resourcemanager.security;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;

import static org.apache.hadoop.metrics2.lib.Interns.info;
import org.apache.hadoop.metrics2.lib.MutableRate;

/**
 * Class to capture the performance metrics of DelegationTokenRenewer.
 * This should be a singleton.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
@Metrics(context="DelegationTokenRenewer-op-durations")
public class DelegationTokenRenewerOpDurations implements MetricsSource {

    @Metric("Duration to handle a delegation token renewer of app submit")
    MutableRate delegationTokenRenewerAppSubmitEventCall;

    @Metric("Duration to handle a delegation token renewer of app recovery")
    MutableRate delegationTokenRenewerAppRecoverEventCall;

    @Metric("Duration to handle a delegation token renewer of app finished")
    MutableRate delegationTokenRenewerAppFinishEventCall;

    protected static final MetricsInfo RECORD_INFO =
        info("DelegationTokenRenewerOpDurations", "Durations of DelegationToken Renewer calls");

    private final MetricsRegistry registry;
    private boolean isExtended = false;

    private static final DelegationTokenRenewerOpDurations INSTANCE
        = new DelegationTokenRenewerOpDurations();

    public static DelegationTokenRenewerOpDurations getInstance(boolean isExtended) {
        INSTANCE.setExtended(isExtended);
        return INSTANCE;
    }

    private DelegationTokenRenewerOpDurations() {
        registry = new MetricsRegistry(RECORD_INFO);
        registry.tag(RECORD_INFO, "DelegationTokenRenewerOpDurations");

        MetricsSystem ms = DefaultMetricsSystem.instance();
        if (ms != null) {
            ms.register(RECORD_INFO.name(), RECORD_INFO.description(), this);
        }
    }


    private synchronized void setExtended(boolean isExtended) {
        if (isExtended == INSTANCE.isExtended)
            return;

        delegationTokenRenewerAppSubmitEventCall.setExtended(isExtended);
        delegationTokenRenewerAppRecoverEventCall.setExtended(isExtended);
        delegationTokenRenewerAppFinishEventCall.setExtended(isExtended);

        INSTANCE.isExtended = isExtended;
    }

    @Override
    public synchronized void getMetrics(MetricsCollector collector, boolean all) {
        registry.snapshot(collector.addRecord(registry.info()), all);
    }

    public void addDelegationTokenRenewerAppSubmitEventDuration(long value) {
        delegationTokenRenewerAppSubmitEventCall.add(value);
    }

    public void addDelegationTokenRenewerAppRecoverEventDuration(long value) {
        delegationTokenRenewerAppRecoverEventCall.add(value);
    }

    public void addDelegationTokenRenewerAppFinishEventDuration(long value) {
        delegationTokenRenewerAppFinishEventCall.add(value);
    }
}
