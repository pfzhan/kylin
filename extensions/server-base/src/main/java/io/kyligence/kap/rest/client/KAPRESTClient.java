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

package io.kyligence.kap.rest.client;

import java.io.IOException;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.kylin.common.restclient.RestClient;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.rest.request.QueryRequest;
import io.kyligence.kap.rest.request.SequenceSQLRequest;
import io.kyligence.kap.rest.request.ShardedSequenceSQLRequest;
import io.kyligence.kap.rest.response.SequenceSQLResponse;

public class KAPRESTClient extends RestClient {

    private static final Logger logger = LoggerFactory.getLogger(KAPRESTClient.class);

    private String basicAuthentication;

    /**
     * @param uri "user:pwd@host:port"
     */
    public KAPRESTClient(String uri, String basicAuthentication) {
        super(uri);
        this.basicAuthentication = basicAuthentication;
    }

    public String massinRequest(String SQL) throws IOException {
        String url = baseUrl + "/massin/query";
        HttpPost post = new HttpPost(url);
        post.addHeader("Authorization", basicAuthentication);

        QueryRequest body = new QueryRequest();
        body.setProject("DEFAULT");
        body.setSql(SQL);
        String requestString = JsonUtil.writeValueAsString(body);
        post.setEntity(new StringEntity(requestString, ContentType.create("application/json", "UTF-8")));
        HttpResponse response = client.execute(post);
        String filterName = EntityUtils.toString(response.getEntity());
        logger.info("filter name is {}", filterName);
        return filterName;
    }

    public SequenceSQLResponse dispatchSequenceSQLExecutionToWorker(int totalWorkers, int workerID,
            SequenceSQLRequest originalRequest) throws IOException {
        long startTime = System.currentTimeMillis();
        ShardedSequenceSQLRequest request = new ShardedSequenceSQLRequest();
        request.setWorkerCount(totalWorkers);
        request.setWorkerID(workerID);
        request.setAcceptPartial(originalRequest.isAcceptPartial());
        request.setBackdoorToggles(originalRequest.getBackdoorToggles());
        request.setLimit(originalRequest.getLimit());
        request.setOffset(originalRequest.getOffset());
        request.setProject(originalRequest.getProject());
        request.setSql(originalRequest.getSql());
        request.setSequenceID(originalRequest.getSequenceID());
        request.setSequenceOpt(originalRequest.getSequenceOpt());
        request.setStepID(originalRequest.getStepID());
        request.setResultOpt(originalRequest.getResultOpt());
        String requestString = JsonUtil.writeValueAsString(request);

        String url = baseUrl + "/shardable_query_worker/execution";
        HttpPost post = new HttpPost(url);
        post.addHeader("Authorization", basicAuthentication);
        post.setEntity(new StringEntity(requestString, ContentType.create("application/json", "UTF-8")));

        try {
            HttpResponse response = client.execute(post);
            String msg = EntityUtils.toString(response.getEntity());

            if (response.getStatusLine().getStatusCode() != 200)
                throw new IOException("Invalid response " + response.getStatusLine().getStatusCode()
                        + " with shardable query  " + url + "\n" + msg);

            SequenceSQLResponse sequenceSQLResponse = JsonUtil.readValue(msg, SequenceSQLResponse.class);
            logger.info("KAPRESTClient {} dispatchSequenceSQLExecutionToWorker finished in {} millis", url,
                    System.currentTimeMillis() - startTime);
            return sequenceSQLResponse;
        } catch (Exception ex) {
            throw new IOException(ex);
        } finally {
            post.releaseConnection();
        }
    }

    public SequenceSQLResponse collectSequenceSQLResultFromWorker(int workerID, long sequenceID) throws IOException {

        long startTime = System.currentTimeMillis();
        String url = baseUrl + "/shardable_query_worker/result/" + sequenceID + "/" + workerID;
        HttpGet get = new HttpGet(url);
        get.addHeader("Authorization", basicAuthentication);

        try {
            HttpResponse response = client.execute(get);
            String msg = EntityUtils.toString(response.getEntity());

            if (response.getStatusLine().getStatusCode() != 200)
                throw new IOException("Invalid response " + response.getStatusLine().getStatusCode()
                        + " when collecting results from  " + url + "\n" + msg);

            SequenceSQLResponse sequenceSQLResponse = JsonUtil.readValue(msg, SequenceSQLResponse.class);
            logger.info("KAPRESTClient {} collectSequenceSQLResultFromWorker finished in {} millis", url,
                    System.currentTimeMillis() - startTime);
            return sequenceSQLResponse;

        } catch (Exception ex) {
            throw new IOException(ex);
        } finally {
            get.releaseConnection();
        }
    }

    public String collectStatsFromWorker(int workerID, long sequenceID) throws IOException {

        long startTime = System.currentTimeMillis();
        String url = baseUrl + "/shardable_query_worker/topology/" + sequenceID + "/" + workerID;
        HttpGet get = new HttpGet(url);
        get.addHeader("Authorization", basicAuthentication);

        try {
            HttpResponse response = client.execute(get);
            String msg = EntityUtils.toString(response.getEntity());

            if (response.getStatusLine().getStatusCode() != 200)
                throw new IOException("Invalid response " + response.getStatusLine().getStatusCode()
                        + " when collecting stats from  " + url + "\n" + msg);

            String ret = msg;
            logger.info("KAPRESTClient {} collectStatsFromWorker finished in {} millis", url,
                    System.currentTimeMillis() - startTime);
            return ret;

        } catch (Exception ex) {
            throw new IOException(ex);
        } finally {
            get.releaseConnection();
        }
    }

    public Pair<String, String> getJobServerState() {
        String url = baseUrl + "/service_discovery/state/is_job_server";
        HttpGet get = new HttpGet(url);
        get.addHeader("Authorization", basicAuthentication);
        try {
            HttpResponse response = client.execute(get);
            String msg = EntityUtils.toString(response.getEntity());

            if (response.getStatusLine().getStatusCode() != 200) {
                logger.error("Invalid response " + response.getStatusLine().getStatusCode()
                        + " when collecting results from  " + url + "\n" + msg);
                msg = "unknown";
            }
            return Pair.newPair(host + ":" + port, msg);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        } finally {
            get.releaseConnection();
        }
    }

    public int retrieveSparkExecutorNum() {
        String url = baseUrl + "/config/spark_status";
        HttpGet get = new HttpGet(url);
        try {
            HttpResponse response = client.execute(get);
            String msg = EntityUtils.toString(response.getEntity());
            if (response.getStatusLine().getStatusCode() != 200)
                throw new IOException("Invalid response " + response.getStatusLine().getStatusCode()
                        + " when collecting results from  " + url + "\n" + msg);

            int execNum = Integer.valueOf(JsonUtil.readValueAsTree(msg).get("data").get("v").asText());

            byte[] bytes = new byte[4];

            BytesUtil.writeUnsigned(execNum, bytes, 0, bytes.length);
            byte tmp = bytes[0];
            bytes[0] = bytes[2];
            bytes[2] = tmp;
            tmp = bytes[1];
            bytes[1] = bytes[3];
            bytes[3] = tmp;
            execNum = Integer.reverse(BytesUtil.readUnsigned(bytes, 0, bytes.length));

            return execNum;
        } catch (Exception ex) {
            return 0;
        } finally {
            get.releaseConnection();
        }
    }
}
