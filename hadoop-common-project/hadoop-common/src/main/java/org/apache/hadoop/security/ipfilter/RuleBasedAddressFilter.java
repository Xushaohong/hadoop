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

import java.net.InetAddress;
import java.net.UnknownHostException;
import org.apache.hadoop.security.AbstractRemoteAddressFilter;
import org.apache.hadoop.security.AccessControlException;

/**
 * Support subnet ip and uniq ip, accept it when one of the rules accepted.
 */
public class RuleBasedAddressFilter extends AbstractRemoteAddressFilter {

    private final SubnetAddressRule subnetRule;
    private final UniqAddressRule uniqRule;

    public RuleBasedAddressFilter() {
        this.subnetRule = new SubnetAddressRule();
        this.uniqRule = new UniqAddressRule();
    }

    @Override
    public void addAddress(String address) throws UnknownHostException {
        if (isSubnet(address)) {
            this.subnetRule.addAddress(address);
        } else {
            this.uniqRule.addAddress(address);
        }
    }

    @Override
    public boolean accept(InetAddress remoteAddress) throws AccessControlException {
        return uniqRule.accept(remoteAddress) || subnetRule.accept(remoteAddress);
    }
}
