/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package io.kyligence.kap.engine.spark.cloud;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;

import io.kyligence.kap.engine.spark.job.SnapshotBuildFinishedEvent;
import io.kyligence.kap.guava20.shaded.common.eventbus.Subscribe;
import io.kyligence.kap.shaded.curator.org.apache.curator.framework.CuratorFramework;
import io.kyligence.kap.shaded.curator.org.apache.curator.framework.CuratorFrameworkFactory;
import io.kyligence.kap.shaded.curator.org.apache.curator.retry.ExponentialBackoffRetry;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AlluxioExtension {

    @Subscribe
    public void onSnapshotFinished(SnapshotBuildFinishedEvent finished) throws Exception {
        if (finished.getSelectedPartCol() == null) {
            return;
        }
        refreshCacheIfNecessary(finished.getTableDesc().getLastSnapshotPath());

    }

    private void refreshCacheIfNecessary(String snapshotPath) throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        if (!KapConfig.wrap(config).isCloud() || config.skipFreshAlluxio()) {
            return;
        }

        String hostName = acquireAlluxioAddress();
        log.info("alluxio hostname: {}", hostName);

        String rootPath = new Path(KapConfig.wrap(config).getMetadataWorkingDirectory()).toUri().getPath();
        String snapshotAbsolutePath = rootPath + "/" + snapshotPath;

        String listUrl = String.format(Locale.ROOT, "HTTP://%s:39999/api/v1/paths/%s/list-status", hostName,
                snapshotAbsolutePath);
        log.info("list url: {}", listUrl);

        // post request
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpPost listAlwaysPost = constructListPost(listUrl, "ALWAYS");
            HttpPost listOncePost = constructListPost(listUrl, "ONCE");
            chainPost(httpClient, listAlwaysPost, listOncePost);
        }
    }

    private void chainPost(CloseableHttpClient httpClient, HttpPost... postRequests) throws IOException {
        for (HttpPost postRequest : postRequests) {
            HttpResponse response = httpClient.execute(postRequest);
            int code = response.getStatusLine().getStatusCode();
            if (code != HttpStatus.SC_OK) {
                InputStream inputStream = response.getEntity().getContent();
                String responseContent = IOUtils.toString(inputStream);
                log.warn("request to url, info: {}", postRequest.getURI(), responseContent);
            }
        }
    }

    private HttpPost constructListPost(String listUrl, String type) {
        HttpPost listPost = new HttpPost(listUrl);
        listPost.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
        listPost.setEntity(
                new StringEntity(String.format(Locale.ROOT, "{\"recursive\":true,\"loadMetadataType\":\"%s\"}", type),
                        StandardCharsets.UTF_8));
        return listPost;
    }

    private String acquireAlluxioAddress() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        String workSpace = getWorkSpace(config);
        log.info("get workspace name : {}", workSpace);

        // zk url
        String zkPath = String.format(Locale.ROOT, "/alluxio/%s/leader", workSpace);
        log.info("zkPath is : {}", zkPath);

        CuratorFramework client = CuratorFrameworkFactory.builder().connectString(config.getZookeeperConnectString())
                .sessionTimeoutMs(3000).connectionTimeoutMs(5000).retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();
        client.start();

        List<String> children = client.getChildren().forPath(zkPath);
        log.info("zk children : " + children.toString());

        String hostName = children.get(0).split(":")[0];
        log.info("get alluxio host {} from zk ", hostName);

        client.close();
        return hostName;
    }

    private String getWorkSpace(KylinConfig config) {
        String workSpace = config.getMetadataUrlPrefix();
        log.info("original workspace is {}", workSpace);
        String workSpaceSuffix = "_kylin";
        if (workSpace.endsWith(workSpaceSuffix)) {
            workSpace = workSpace.substring(0, workSpace.length() - workSpaceSuffix.length());
        }
        return workSpace;
    }
}
