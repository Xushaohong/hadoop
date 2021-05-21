#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

hadoop_add_profile odfs

function _odfs_hadoop_classpath
{
  #
  # get all of the odfs jars+config in the path
  #
  # developers
  if [[ -n "${HADOOP_ENABLE_BUILD_PATHS}" ]]; then
    hadoop_add_classpath "${HADOOP_ODFS_HOME}/hadoop-odfs/target/classes"
  fi

  # put odfs in classpath if present
  if [[ -d "${HADOOP_ODFS_HOME}/${ODFS_DIR}/webapps" ]]; then
    hadoop_add_classpath "${HADOOP_ODFS_HOME}/${ODFS_DIR}"
  fi

  hadoop_add_classpath "${HADOOP_ODFS_HOME}/${ODFS_LIB_JARS_DIR}"'/*'
  hadoop_add_classpath "${HADOOP_ODFS_HOME}/${ODFS_DIR}"'/*'
}
