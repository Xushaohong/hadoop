/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.util.NodeManagerHardwareUtils;
import org.apache.hadoop.yarn.util.ResourceCalculatorPlugin;
import org.apache.hadoop.yarn.util.LinuxResourceCalculatorPlugin;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 * An implementation for using CGroups to restrict CPU usage on some of cores
 * on Linux. This is implemented using cpuset.cpus to restrict usage of all YARN
 * containers.
 *
 */
@InterfaceStability.Unstable
@InterfaceAudience.Private
public class CGroupsCpuSetResourceHandlerImpl implements CpuSetResourceHandler {

    static final Logger LOG =
            LoggerFactory.getLogger(CGroupsCpuSetResourceHandlerImpl.class);

    private CGroupsHandler cGroupsHandler;
    private float yarnProcessors;
    private String cpusets;
    private String cpumems;
    private String totalCpus;
    private String cpusetPath;
    private static final CGroupsHandler.CGroupController CPUSET =
            CGroupsHandler.CGroupController.CPUSET;

    CGroupsCpuSetResourceHandlerImpl(CGroupsHandler cGroupsHandler) {
        this.cGroupsHandler = cGroupsHandler;
    }

    @Override
    public List<PrivilegedOperation> bootstrap(Configuration conf)
            throws ResourceHandlerException {
        return bootstrap(
                new LinuxResourceCalculatorPlugin(), conf);
    }

    @VisibleForTesting
    List<PrivilegedOperation> bootstrap(
            ResourceCalculatorPlugin plugin, Configuration conf)
            throws ResourceHandlerException {
        this.cGroupsHandler.initializeCGroupController(CPUSET);
        cpusetPath = cGroupsHandler.getControllerPath(CPUSET);

        boolean cpusetEnabled = conf.getBoolean(
                YarnConfiguration.NM_RESOURCE_LIMITED_PHYSICAL_CPU_SET,
                YarnConfiguration.DEFAULT_NM_RESOURCE_LIMITED_PHYSICAL_CPU_SET);

        // get total cpuset.cpus value
        try {
            totalCpus = getTotalCpus();
        } catch (IOException ie) {
            throw new ResourceHandlerException(ie);
        }

        if (cpusetEnabled) {
            LinuxResourceCalculatorPlugin lxplugin = (LinuxResourceCalculatorPlugin) plugin;
            // cap overall usage to the number of cores allocated to YARN
            yarnProcessors = (lxplugin.getNumProcessors() * NodeManagerHardwareUtils.getNodeCpuPercentage(conf)) / 100.0f;

            HashMap<Integer, ArrayList<Integer>> systemProcessors = lxplugin.getCpuSets();
            cpusets = getRestrictCpus(systemProcessors, (int) yarnProcessors);
        } else {
            cpusets = totalCpus;
        }

        // get cpuset.mems value
        try {
            cpumems = getTotalMems();
        } catch (IOException ie) {
            throw new ResourceHandlerException(ie);
        }

        if (cpusets == "" || cpumems == "")
            throw new IllegalArgumentException("Got error: restrictMems=" + cpusets + ", restrictMems=" + cpumems);
        LOG.info("Yarn containers will restricting to cpus:" + cpusets + ", mems:" + cpumems);

        // updating hadoop-yarn with total cpus, just limiting cpus in container level.
        cGroupsHandler.updateCGroupParam(CPUSET, "", CGroupsHandler.CGROUP_CPUSET_CPUS, totalCpus);
        cGroupsHandler.updateCGroupParam(CPUSET, "", cGroupsHandler.CGROUP_CPUSET_MEMS, cpumems);

        boolean exited;
        try {
            exited = totalCpuSetExisted(totalCpus);
        } catch (IOException ie) {
            throw new ResourceHandlerException(ie);
        }
        if (!exited) {
            // Check if set successfully
            throw new ResourceHandlerException("cpuset limit set failed: " + cpusets);
        }

        return null;
    }

    @InterfaceAudience.Private
    private boolean totalCpuSetExisted(String existedCpus) throws IOException {
        String controllerPath = this.cGroupsHandler.getPathForCGroup(CPUSET, "");
        File cpusFile = new File(controllerPath, CPUSET + "." + this.cGroupsHandler.CGROUP_CPUSET_CPUS);
        if (cpusFile.exists()) {
            String contents = FileUtils.readFileToString(cpusFile, "UTF-8").trim();
            if (contents.equals(existedCpus)) {
                return true;
            }
        }
        return false;
    }

    @InterfaceAudience.Private
    private String getRestrictCpus(HashMap<Integer, ArrayList<Integer>> systemCpuSets, int processors) {
        String limitedCpus;
        ArrayList<Integer> cpulist;
        if (processors == 0)
            throw new IllegalArgumentException("The cpuset num is 0!");

        cpulist = new ArrayList<Integer>();
        for (ArrayList<Integer> processorlist : systemCpuSets.values()) {
            int processnum = Math.min(processorlist.size(), processors);
            if (processnum == 0)
                break;
            for (int i = 0; i < processnum; i++) {
                cpulist.add(processorlist.get(i));
                processors--;
            }
        }

        // sort the cpu id and format it, such as "0-2,5-9,10...."
        int cur, rbot, rtop;
        Collections.sort(cpulist);
        // we could maker sure the cpulist has at least one element
        cur = cpulist.get(0);
        rbot = cur;
        limitedCpus = "";

        int i = 0;
        while (i < cpulist.size() + 1) {
            rtop = cur;
            if ( i < cpulist.size() )
                cur = cpulist.get(i);
            else
                cur = -1;

            if (cur == -1 || cur > rtop + 1) {
                if (limitedCpus != "")
                    limitedCpus = limitedCpus + ",";
                if (rbot == rtop)
                    limitedCpus = limitedCpus + String.valueOf(rbot);
                else
                    limitedCpus = limitedCpus + String.valueOf(rbot) + "-" + String.valueOf(rtop);
                rbot = cur;
            }
            i++;
        }

        return limitedCpus;
    }

    // clone from parent
    @InterfaceAudience.Private
    private String getTotalMems() throws IOException {
        File memsFile = new File(this.cpusetPath, CPUSET.getName() + "." + this.cGroupsHandler.CGROUP_CPUSET_MEMS);
        if (memsFile.exists())
            return FileUtils.readFileToString(memsFile, "UTF-8").trim();
        else
            throw new IOException("cpuset.mems not existed in path:" + this.cpusetPath);
    }

    // clone from parent
    @InterfaceAudience.Private
    private String getTotalCpus() throws IOException {
        File cpusFile = new File(this.cpusetPath, CPUSET.getName() + "." + this.cGroupsHandler.CGROUP_CPUSET_CPUS);
        if (cpusFile.exists())
            return FileUtils.readFileToString(cpusFile, "UTF-8").trim();
        else
            throw new IOException("cpuset.cpus not existed in path:" + this.cpusetPath);
    }

    @Override
    public List<PrivilegedOperation> preStart(Container container)
            throws ResourceHandlerException {
        String cgroupId = container.getContainerId().toString();
        cGroupsHandler.createCGroup(CPUSET, cgroupId);
        updateContainer(container);
        List<PrivilegedOperation> ret = new ArrayList<>();
        ret.add(new PrivilegedOperation(
                PrivilegedOperation.OperationType.ADD_PID_TO_CGROUP,
                PrivilegedOperation.CGROUP_ARG_PREFIX + cGroupsHandler
                        .getPathForCGroupTasks(CPUSET, cgroupId)));
        return ret;
    }

    @Override
    public List<PrivilegedOperation> reacquireContainer(ContainerId containerId)
            throws ResourceHandlerException {
        return null;
    }

    @Override
    public List<PrivilegedOperation> updateContainer(Container container)
            throws ResourceHandlerException {
        String cgroupId = container.getContainerId().toString();
        File cgroup = new File(cGroupsHandler.getPathForCGroup(CPUSET, cgroupId));
        if (cgroup.exists()) {
            try {
                cGroupsHandler.updateCGroupParam(CPUSET, cgroupId,
                        CGroupsHandler.CGROUP_CPUSET_CPUS, this.cpusets);
                cGroupsHandler.updateCGroupParam(CPUSET, cgroupId,
                        CGroupsHandler.CGROUP_CPUSET_MEMS, this.cpumems);

            } catch (ResourceHandlerException re) {
                cGroupsHandler.deleteCGroup(CPUSET, cgroupId);
                LOG.warn("Could not update cpuset cgroup for container", re);
                throw re;
            }
        }
        return null;
    }


    @Override
    public List<PrivilegedOperation> postComplete(ContainerId containerId)
            throws ResourceHandlerException {
        cGroupsHandler.deleteCGroup(CPUSET, containerId.toString());
        return null;
    }

    @Override public List<PrivilegedOperation> teardown()
            throws ResourceHandlerException {
        return null;
    }

    @Override
    public String toString() {
        return CGroupsCpuSetResourceHandlerImpl.class.getName();
    }
}