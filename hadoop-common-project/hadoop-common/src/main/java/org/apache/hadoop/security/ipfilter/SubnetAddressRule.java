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

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.shaded.com.google.common.net.InetAddresses;
import org.apache.hadoop.security.AddressFilterRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ip filter to check subnet ip rules.
 */
public class SubnetAddressRule implements AddressFilterRule {


    public static final Logger LOG =
            LoggerFactory.getLogger(SubnetAddressRule.class);

    private static final int MIN_SEG = 16;
    private static final int ADDRESS_NUM = 2;

    private static class SubnetInfo {
        private final int subnetMask;
        private final int address;

        public SubnetInfo(int subnetMask, int address) {
            this.subnetMask = subnetMask;
            this.address = address;
        }

        @Override
        public int hashCode() {
            return Objects.hash(subnetMask, address);
        }

        public boolean equals(SubnetInfo info) {
            return address == info.address && subnetMask == info.subnetMask;
        }
    }

    public static int getInt(byte[] arr, int off) {
        return arr[off]<<8 &0xFF00 | arr[off+1]&0xFF;
    }

    private final Map<Integer, Set<SubnetInfo>> subnetInfos = new HashMap<>();

    @Override
    public void addAddress(String address) throws UnknownHostException {
        String[] addressList = StringUtils.split(address, "/");
        if (addressList == null || addressList.length != ADDRESS_NUM) {
            throw new UnknownHostException(address + " is not a valid address");
        }

        // only support B seg and C seg subnet ip.
        int number = Integer.parseInt(addressList[1]);
        if (number < MIN_SEG) {
            throw new UnknownHostException(address + " is not a valid address");
        }
        int subnetMask = (int) ((-1L << 32 - number));
        int addressNum = InetAddresses.coerceToInteger(InetAddress.getByName(addressList[0])) & subnetMask;
        String[] addresses = StringUtils.split(addressList[0], ".");
        byte[] uniqNumArr = {
                (byte) (Integer.parseInt(addresses[0])),
                (byte) (Integer.parseInt(addresses[1]))
        };
        int uniqNum = getInt(uniqNumArr, 0);
        Set<SubnetInfo> infos = subnetInfos.get(uniqNum);
        if (infos == null) {
            infos = new HashSet<>();
        }
        infos.add(new SubnetInfo(subnetMask, addressNum));
        subnetInfos.put(uniqNum, infos);
    }

    @Override
    public boolean accept(InetAddress remoteAddress) {
        // right now only support v4 subnet.
        if (remoteAddress instanceof Inet4Address) {
            int ipAddress = InetAddresses.coerceToInteger(remoteAddress);
            int uniqNum = getInt(ArrayUtils.subarray(remoteAddress.getAddress(),0, 2), 0);
            Set<SubnetInfo> infos = subnetInfos.get(uniqNum);
            if (infos == null) {
                return false;
            }
            for (SubnetInfo info : infos) {
                boolean isMatch = (ipAddress & info.subnetMask) == info.address;
                if (isMatch) {
                    return true;
                }
            }
        }
        return false;
    }
}
