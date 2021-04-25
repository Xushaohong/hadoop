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
package org.apache.hadoop.yarn.server.nodemanager.webapp.dao;

import org.apache.hadoop.yarn.api.records.Resource;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "capacity")
@XmlAccessorType(XmlAccessType.FIELD)
public class CapacityInfo {

  protected int cores;
  protected long memMb;

  public CapacityInfo() {
  } // JAXB needs this

  public CapacityInfo(Resource resource) {
    this.cores = resource.getVirtualCores();
    this.memMb = resource.getMemorySize();
  }

  public int getCores() { return this.cores; }

  public long getMemMb() { return this.memMb; }
}
