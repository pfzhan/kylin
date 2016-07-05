/*
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

package io.kyligence.kap.rest.client;

import java.io.IOException;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.kylin.common.restclient.RestClient;
import org.apache.kylin.common.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public SequenceSQLResponse dispatchSequenceSQLExecutionToWorker(int totalWorkers, int workerID, SequenceSQLRequest originalRequest) throws IOException {
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
            CloseableHttpResponse response = client.execute(post);
            String msg = EntityUtils.toString(response.getEntity());

            if (response.getStatusLine().getStatusCode() != 200)
                throw new IOException("Invalid response " + response.getStatusLine().getStatusCode() + " with shardable query  " + url + "\n" + msg);

            SequenceSQLResponse sequenceSQLResponse = JsonUtil.readValue(msg, SequenceSQLResponse.class);
            logger.info("KAPRESTClient {} dispatchSequenceSQLExecutionToWorker finished in {} millis", url, System.currentTimeMillis() - startTime);
            response.close();
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
            CloseableHttpResponse response = client.execute(get);
            String msg = EntityUtils.toString(response.getEntity());

            if (response.getStatusLine().getStatusCode() != 200)
                throw new IOException("Invalid response " + response.getStatusLine().getStatusCode() + " when collecting results from  " + url + "\n" + msg);

            SequenceSQLResponse sequenceSQLResponse = JsonUtil.readValue(msg, SequenceSQLResponse.class);
            logger.info("KAPRESTClient {} collectSequenceSQLResultFromWorker finished in {} millis", url, System.currentTimeMillis() - startTime);
            response.close();
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
            CloseableHttpResponse response = client.execute(get);
            String msg = EntityUtils.toString(response.getEntity());

            if (response.getStatusLine().getStatusCode() != 200)
                throw new IOException("Invalid response " + response.getStatusLine().getStatusCode() + " when collecting stats from  " + url + "\n" + msg);

            String ret = msg;
            logger.info("KAPRESTClient {} collectStatsFromWorker finished in {} millis", url, System.currentTimeMillis() - startTime);
            response.close();
            return ret;

        } catch (Exception ex) {
            throw new IOException(ex);
        } finally {
            get.releaseConnection();
        }
    }
}
