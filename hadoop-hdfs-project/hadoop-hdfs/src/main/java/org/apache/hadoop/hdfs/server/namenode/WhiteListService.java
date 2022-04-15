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

package org.apache.hadoop.hdfs.server.namenode;

import static org.apache.hadoop.security.ipfilter.FilterConstants.TQ_AUTH_WHITELIST_REQUEST_INTERVAL;
import static org.apache.hadoop.security.ipfilter.FilterConstants.TQ_AUTH_WHITELIST_REQUEST_INTERVAL_DEFAULT;
import static org.apache.hadoop.security.ipfilter.FilterConstants.TQ_AUTH_WHITELIST_REQUEST_PASSWORD;
import static org.apache.hadoop.security.ipfilter.FilterConstants.TQ_AUTH_WHITELIST_REQUEST_URL;
import static org.apache.hadoop.security.ipfilter.FilterConstants.TQ_AUTH_WHITELIST_REQUEST_USER;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.squareup.okhttp.HttpUrl;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.security.AbstractRemoteAddressFilter;
import org.apache.hadoop.security.ipfilter.RuleBasedAddressFilter;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.JvmPauseMonitor;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * White list worker is used to request ip/subnet ip list from
 * auth center, then client rpc server use it to filter client ip.
 * Only those clients which can be accepted by ip list have permission
 * to request namenode service.
 */
public class WhiteListService extends AbstractService {

    private static final Logger LOG =
            LoggerFactory.getLogger(WhiteListService.class);


    private long workerInterval;
    private String httpUserName;
    private String httpPassword;
    private HttpUrl httpUrl;
    private final OkHttpClient client = new OkHttpClient();

    private volatile boolean shouldRun = true;

    private Thread workerThread;

    private final NameNode nameNode;


    public WhiteListService(NameNode nameNode) {
        super(JvmPauseMonitor.class.getName());
        this.nameNode = nameNode;
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        this.workerInterval = conf.getLong(TQ_AUTH_WHITELIST_REQUEST_INTERVAL,
                TQ_AUTH_WHITELIST_REQUEST_INTERVAL_DEFAULT);
        this.httpUserName = conf.get(TQ_AUTH_WHITELIST_REQUEST_USER);
        this.httpPassword = conf.get(TQ_AUTH_WHITELIST_REQUEST_PASSWORD);
        String urlStr = conf.get(TQ_AUTH_WHITELIST_REQUEST_URL);
        Preconditions.checkArgument(this.httpUserName != null, TQ_AUTH_WHITELIST_REQUEST_USER
                + " should be configured if white list is enabled");
        Preconditions.checkArgument(this.httpPassword != null, TQ_AUTH_WHITELIST_REQUEST_PASSWORD
                + " should be configured if white list is enabled");
        Preconditions.checkArgument(urlStr != null, TQ_AUTH_WHITELIST_REQUEST_URL
                + " should be configured if white list is enabled");

        String serviceId = DFSUtil.getNamenodeNameServiceId(conf);
        this.httpUrl = HttpUrl.parse(urlStr).newBuilder()
                .addPathSegment(serviceId)
                .build();
        LOG.info("White list url is {}", httpUrl);
        this.client.setConnectTimeout(URLConnectionFactory.DEFAULT_SOCKET_TIMEOUT,
                TimeUnit.MILLISECONDS);
        this.client.setReadTimeout(URLConnectionFactory.DEFAULT_SOCKET_TIMEOUT,
                TimeUnit.MILLISECONDS);
        super.serviceInit(conf);
    }

    @Override
    protected void serviceStart() throws Exception {
        workerThread = new Daemon(new Worker());
        workerThread.start();
        super.serviceStart();
    }

    @Override
    protected void serviceStop() throws Exception {
        shouldRun = false;
        if (workerThread != null) {
            workerThread.interrupt();
            try {
                workerThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        super.serviceStop();
    }


    /**
     * json object from auth center.
     */
    private static class FilterResponse {
        private String success;
        private List<String> data;
        private String traceId;
    }

    /**
     * interval request auth center to get latest white list.
     */
    private class Worker implements Runnable {

        private final Gson gson = new Gson();

        private void doRunLoop() {
            LOG.info("Starting to get white list from auth center.");
            Request request = new Request.Builder()
                    .header("username", httpUserName)
                    .header("password", httpPassword)
                    .url(httpUrl).build();
            while (shouldRun) {
                try {
                    // with (0, 20s) addition sleep to avoid massive requests on the same time.
                    long currentInterval = RandomUtils.nextLong(workerInterval, 20000 + workerInterval);
                    try {
                        Response response = client.newCall(request).execute();
                        if (response.code() == HttpStatus.SC_OK) {
                            String bodyStr = response.body().string();
                            FilterResponse filterResponse = gson.fromJson(bodyStr, FilterResponse.class);
                            AbstractRemoteAddressFilter addressFilter = new RuleBasedAddressFilter();
                            for (String address : filterResponse.data) {
                                LOG.debug("address is {}", address);
                                addressFilter.addAddress(address);
                            }
                            nameNode.updateWhiteListForRpc(addressFilter);
                        }
                    } catch (UnknownHostException ex) {
                        LOG.error("Got invalid address {}, ignore it", ex.getMessage());
                    } catch (Throwable ex) {
                        LOG.error("Exception while requesting {}", httpUrl, ex);
                    }
                    TimeUnit.MILLISECONDS.sleep(currentInterval);
                } catch (InterruptedException ignored) {
                    LOG.warn("White list worker interrupted");
                } catch (Throwable e) {
                    LOG.error("Exception caught while running white list worker", e);
                }
            }
        }

        @Override
        public void run() {
            try {
                doRunLoop();
            } finally {
                LOG.info("White list worker stop running.");
            }
        }
    }
}
