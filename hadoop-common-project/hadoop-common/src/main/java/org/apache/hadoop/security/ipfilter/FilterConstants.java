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
package org.apache.hadoop.security.ipfilter;

public class FilterConstants {

    // whether support falling back to simple mode.
    public static final String TQ_AUTH_COMPATIBLE = "tq.auth.compatible";
    public static final boolean TQ_AUTH_COMPATIBLE_DEFAULT = false;

    // whether enable white list
    public static final String TQ_AUTH_WHITELIST_ENABLED = "tq.auth.whitelist.enabled";
    public static final boolean TQ_AUTH_WHITELIST_ENABLED_DEFAULT = false;

    // enabled port, white list right now only works on port 9000
    public static final String TQ_AUTH_WHITELIST_PORT = "tq.auth.whitelist.port";
    public static final int TQ_AUTH_WHITELIST_PORT_DEFAULT = 9000;

    // request interval in ms
    public static final String TQ_AUTH_WHITELIST_REQUEST_INTERVAL = "tq.auth.whitelist.request.interval";
    public static final long TQ_AUTH_WHITELIST_REQUEST_INTERVAL_DEFAULT = 5 * 60 * 1000;

    public static final String TQ_AUTH_WHITELIST_REQUEST_USER = "tq.auth.whitelist.request.user";
    public static final String TQ_AUTH_WHITELIST_REQUEST_PASSWORD = "tq.auth.whitelist.request.password";

    public static final String TQ_AUTH_WHITELIST_REQUEST_URL = "tq.auth.whitelist.request.url";
}
