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

import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.kylin.common.restclient.RestClient;
import org.apache.kylin.common.util.Bytes;
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
        PostMethod post = new PostMethod(url);
        post.addRequestHeader("Authorization", basicAuthentication);
        post.setRequestEntity(new StringRequestEntity(requestString, "application/json", "UTF-8"));

        try {
            int code = client.executeMethod(post);
            String msg = Bytes.toString(post.getResponseBody());

            if (code != 200)
                throw new IOException("Invalid response " + code + " with shardable query  " + url + "\n" + msg);

            SequenceSQLResponse sequenceSQLResponse = JsonUtil.readValue(msg, SequenceSQLResponse.class);
            logger.info("KAPRESTClient {} dispatchSequenceSQLExecutionToWorker finished in {} millis", url, System.currentTimeMillis() - startTime);
            return sequenceSQLResponse;

        } catch (HttpException ex) {
            throw new IOException(ex);
        } finally {
            post.releaseConnection();
        }
    }

    public SequenceSQLResponse collectSequenceSQLResultFromWorker(int workerID, long sequenceID) throws IOException {

        long startTime = System.currentTimeMillis();
        String url = baseUrl + "/shardable_query_worker/result/" + sequenceID + "/" + workerID;
        HttpMethod get = new GetMethod(url);
        get.addRequestHeader("Authorization", basicAuthentication);

        try {
            int code = client.executeMethod(get);
            String msg = get.getResponseBodyAsString();

            if (code != 200)
                throw new IOException("Invalid response " + code + " when collecting results from  " + url + "\n" + msg);

            SequenceSQLResponse sequenceSQLResponse = JsonUtil.readValue(msg, SequenceSQLResponse.class);
            logger.info("KAPRESTClient {} collectSequenceSQLResultFromWorker finished in {} millis", url, System.currentTimeMillis() - startTime);
            return sequenceSQLResponse;

        } catch (HttpException ex) {
            throw new IOException(ex);
        } finally {
            get.releaseConnection();
        }
    }

    public String collectStatsFromWorker(int workerID, long sequenceID) throws IOException {

        long startTime = System.currentTimeMillis();
        String url = baseUrl + "/shardable_query_worker/topology/" + sequenceID + "/" + workerID;
        HttpMethod get = new GetMethod(url);
        get.addRequestHeader("Authorization", basicAuthentication);

        try {
            int code = client.executeMethod(get);
            String msg = get.getResponseBodyAsString();

            if (code != 200)
                throw new IOException("Invalid response " + code + " when collecting stats from  " + url + "\n" + msg);

            String ret = msg;
            logger.info("KAPRESTClient {} collectStatsFromWorker finished in {} millis", url, System.currentTimeMillis() - startTime);
            return ret;

        } catch (HttpException ex) {
            throw new IOException(ex);
        } finally {
            get.releaseConnection();
        }
    }
}
