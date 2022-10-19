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

package org.apache.hadoop.yarn.server.applicationhistoryservice;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationAttemptFinishData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationAttemptHistoryData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationAttemptStartData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationFinishData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationHistoryData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationStartData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ContainerFinishData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ContainerHistoryData;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ContainerStartData;

import java.io.IOException;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;

/**
 * Dummy implementation of {@link ApplicationHistoryStore}.
 */
@Unstable
@Private
public class LogApplicationHistoryStore extends AbstractService implements
    ApplicationHistoryStore {

  private static final Log LOG = LogFactory.getLog(LogApplicationHistoryStore.class);

  private static final String DELIMETER = "|";

  private static final String APP_START = "AT";
  private static final String APP_STOP = "AP";

  private static final String CONTAINER_START = "CT";
  private static final String CONTAINER_STOP = "CP";

  private static final String APP_ATTEMPT_START = "AAT";
  private static final String APP_ATTEMPT_STOP = "AAP";

  public LogApplicationHistoryStore() {
    super(LogApplicationHistoryStore.class.getName());
  }

  @Override
  public void applicationStarted(ApplicationStartData appStart)
      throws IOException {
    StringBuffer sb =  new StringBuffer(APP_START+DELIMETER);
    String appName = appStart.getApplicationName();
    if (appName != null){
      appName =  appName.replaceAll("|","");
      appName =  appName.replaceAll("\n","");
    }
    sb.append(appStart.getApplicationId())
      .append(DELIMETER)
      .append(appName)
      .append(DELIMETER)
      .append(appStart.getQueue())
      .append(DELIMETER)
      .append(appStart.getApplicationType())
      .append(DELIMETER)
      .append(appStart.getStartTime())
      .append(DELIMETER)
      .append(appStart.getSubmitTime())
      .append(DELIMETER)
      .append(appStart.getUser());
    LOG.info(sb.toString());
  }

  @Override
  public void applicationFinished(ApplicationFinishData appFinish)
      throws IOException {
    StringBuffer sb =  new StringBuffer(APP_STOP+DELIMETER);

    String diagnosticsInfo = processDiagnosticsInfo(appFinish.getDiagnosticsInfo());

    sb.append(appFinish.getApplicationId())
      .append(DELIMETER)
      .append(appFinish.getFinishTime())
      .append(DELIMETER)
      .append(appFinish.getLaunchTime())
      .append(DELIMETER)
      .append(appFinish.getYarnApplicationState())
      .append(DELIMETER)
      .append(appFinish.getFinalApplicationStatus())
      .append(DELIMETER)
      .append(diagnosticsInfo);
    LOG.info(sb.toString());
  }

  @Override
  public void applicationAttemptStarted(
      ApplicationAttemptStartData appAttemptStart) throws IOException {
    StringBuffer sb =  new StringBuffer(APP_ATTEMPT_START+DELIMETER);
    sb.append(appAttemptStart.getApplicationAttemptId())
      .append(DELIMETER)
      .append(appAttemptStart.getMasterContainerId())
      .append(DELIMETER)
      .append(appAttemptStart.getHost())
      .append(DELIMETER)
      .append(System.currentTimeMillis());
    LOG.info(sb.toString());
  }

  @Override
  public void applicationAttemptFinished(
      ApplicationAttemptFinishData appAttemptFinish) throws IOException {
    StringBuffer sb =  new StringBuffer(APP_ATTEMPT_STOP+DELIMETER);

    String diagnosticsInfo = processDiagnosticsInfo(appAttemptFinish.getDiagnosticsInfo());

    sb.append(appAttemptFinish.getApplicationAttemptId())
      .append(DELIMETER)
      .append(appAttemptFinish.getFinalApplicationStatus())
      .append(DELIMETER)
      .append(appAttemptFinish.getYarnApplicationAttemptState())
      .append(DELIMETER)
      .append(diagnosticsInfo)
      .append(DELIMETER)
      .append(System.currentTimeMillis());
    LOG.info(sb.toString());
  }

  @Override
  public void containerStarted(ContainerStartData containerStart)
      throws IOException {
    StringBuffer sb =  new StringBuffer(CONTAINER_START+DELIMETER);
    sb.append(containerStart.getContainerId())
      .append(DELIMETER)
      .append(containerStart.getAllocatedResource().getVirtualCores())
      .append(DELIMETER)
      .append(containerStart.getAllocatedResource().getMemorySize())
      .append(DELIMETER)
      .append(containerStart.getPriority())
      .append(DELIMETER)
      .append(containerStart.getStartTime())
      .append(DELIMETER)
      .append(containerStart.getAssignedNode());
    LOG.info(sb.toString());
  }

  @Override
  public void containerFinished(ContainerFinishData containerFinish)
      throws IOException {
    StringBuffer sb =  new StringBuffer(CONTAINER_STOP+DELIMETER);

    String diagnosticsInfo = processDiagnosticsInfo(containerFinish.getDiagnosticsInfo());

    sb.append(containerFinish.getContainerId())
      .append(DELIMETER)
      .append(containerFinish.getContainerExitStatus())
      .append(DELIMETER)
      .append(containerFinish.getContainerState())
      .append(DELIMETER)
      .append(containerFinish.getFinishTime())
      .append(DELIMETER)
      .append(containerFinish.getAssignedNode())
      .append(DELIMETER)
      .append(diagnosticsInfo);
    LOG.info(sb.toString());
  }

  public String processDiagnosticsInfo(String diagnosticsInfo) {
    String tmpDiagnosticsInfo = diagnosticsInfo;
    if (diagnosticsInfo != null) {
      byte[] diagnosticsInfoBytes = tmpDiagnosticsInfo.getBytes();
      tmpDiagnosticsInfo = Base64.getEncoder().encodeToString(diagnosticsInfoBytes);
    }
    return tmpDiagnosticsInfo;
  }

  @Override
  public ApplicationHistoryData getApplication(ApplicationId appId)
      throws IOException {
    return null;
  }

  @Override
  public Map<ApplicationId, ApplicationHistoryData> getAllApplications()
      throws IOException {
    return Collections.emptyMap();
  }

  @Override
  public Map<ApplicationAttemptId, ApplicationAttemptHistoryData>
      getApplicationAttempts(ApplicationId appId) throws IOException {
    return Collections.emptyMap();
  }

  @Override
  public ApplicationAttemptHistoryData getApplicationAttempt(
      ApplicationAttemptId appAttemptId) throws IOException {
    return null;
  }

  @Override
  public ContainerHistoryData getContainer(ContainerId containerId)
      throws IOException {
    return null;
  }

  @Override
  public ContainerHistoryData getAMContainer(ApplicationAttemptId appAttemptId)
      throws IOException {
    return null;
  }

  @Override
  public Map<ContainerId, ContainerHistoryData> getContainers(
      ApplicationAttemptId appAttemptId) throws IOException {
    return Collections.emptyMap();
  }

}