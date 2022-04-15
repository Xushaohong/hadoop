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
package org.apache.hadoop.security;

import java.net.InetAddress;
import java.net.UnknownHostException;
import org.apache.commons.lang3.StringUtils;

/**
 * Abstract class for address filter.
 */
public abstract class AbstractRemoteAddressFilter {


    /**
     * check whether address is subnet.
     * @param address
     * @return
     */
    public boolean isSubnet(String address) {
        if (address != null) {
            return StringUtils.contains(address,  "/");
        }
        return false;
    }

    /**
     * Add white address to filter rule.
     * @param address - string of address.
     */
    public abstract void addAddress(String address) throws UnknownHostException;

    /**
     * Whether accept remote address.
     * @param remoteAddress - remote address
     * @return true if accept else false.
     * @throws AccessControlException
     */
    public abstract boolean accept(InetAddress remoteAddress) throws AccessControlException;
}
