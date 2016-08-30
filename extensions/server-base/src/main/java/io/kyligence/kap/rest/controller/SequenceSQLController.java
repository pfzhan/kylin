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

package io.kyligence.kap.rest.controller;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import javax.annotation.Nullable;
import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.common.util.LoggableCachedThreadPool;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.service.QueryService;
import org.apache.kylin.rest.util.QueryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.common.util.ValueIterators;
import io.kyligence.kap.rest.client.KAPRESTClient;
import io.kyligence.kap.rest.request.SequenceSQLRequest;
import io.kyligence.kap.rest.request.ShardedSequenceSQLRequest;
import io.kyligence.kap.rest.response.SequenceSQLResponse;
import io.kyligence.kap.rest.sequencesql.DiskResultCache;
import io.kyligence.kap.rest.sequencesql.SequenceNodeOutput;
import io.kyligence.kap.rest.sequencesql.SequenceOpt;
import io.kyligence.kap.rest.sequencesql.topology.SequenceTopology;
import io.kyligence.kap.rest.sequencesql.topology.SequenceTopologyManager;

@Controller
public class SequenceSQLController extends BasicController {

    private static final Logger logger = LoggerFactory.getLogger(SequenceSQLController.class);

    @Autowired
    private QueryService queryService;

    private static ExecutorService executorService = new LoggableCachedThreadPool();
    private static SequenceTopologyManager topologyManager = new SequenceTopologyManager(new DiskResultCache(), KylinConfig.getInstanceFromEnv().getSequenceExpireTime());

    @PostConstruct
    public void init() throws IOException {
    }

    private List<KAPRESTClient> getWorkerClients(String basicAuthen) {
        final String[] servers = KylinConfig.getInstanceFromEnv().getRestServers();
        final List<KAPRESTClient> restClient = Lists.newArrayList();
        final int workerCount = servers.length * KylinConfig.getInstanceFromEnv().getWorkersPerServer();
        for (int i = 0; i < workerCount; i++) {
            logger.info("worker {} : {}", i, servers[i % servers.length]);
            restClient.add(new KAPRESTClient(servers[i % servers.length], basicAuthen));
        }
        return restClient;
    }

    @RequestMapping(value = "/sequence_sql/execution", method = RequestMethod.POST)
    @ResponseBody
    public SequenceSQLResponse doSequenceSql(@RequestBody final SequenceSQLRequest sqlRequest, @RequestHeader("Authorization") String basicAuthen) {
        try {
            long startTime = System.currentTimeMillis();
            if (sqlRequest.getSequenceID() == -1) {
                throw new RuntimeException("Must provided a unique sequenceID for a SQL sequence");
            }

            if (sqlRequest.getSequenceOpt() != SequenceOpt.UPDATE) {
                if (sqlRequest.getStepID() != -1) {
                    throw new RuntimeException("If you're not updating a certain sql, you should leave stepID as default (-1)");
                }

                if (sqlRequest.getSequenceOpt() == SequenceOpt.APPEND) {
                    if (sqlRequest.getSql() == null || sqlRequest.getResultOpt() == null) {
                        throw new RuntimeException("sql and result opt are required");
                    }
                }

                if (sqlRequest.getSequenceOpt() == SequenceOpt.INIT) {
                    if (sqlRequest.getSql() == null)
                        throw new RuntimeException("sql is required");
                }
            }

            if (sqlRequest.getSequenceOpt() == SequenceOpt.UPDATE) {
                if (sqlRequest.getStepID() == -1) {
                    throw new RuntimeException("If you're updating a certain sql, you should provide a existing stepID");
                }
                if (sqlRequest.getStepID() == 0 && sqlRequest.getResultOpt() != null) {
                    throw new RuntimeException("Result opt cannot be updated for step 0");
                }
                if (sqlRequest.getSql() == null && sqlRequest.getResultOpt() == null) {
                    throw new RuntimeException("at least provide sql or result opt");
                }
            }

            final List<KAPRESTClient> workerClients = getWorkerClients(basicAuthen);
            List<SequenceSQLResponse> shardResults = Lists.newArrayList();
            List<Future<?>> futures = Lists.newArrayList();
            for (int i = 0; i < workerClients.size(); i++) {
                final int workerID = i;
                futures.add((executorService.submit(new Callable<SequenceSQLResponse>() {
                    @Override
                    public SequenceSQLResponse call() throws Exception {
                        try {
                            return workerClients.get(workerID).dispatchSequenceSQLExecutionToWorker(workerClients.size(), workerID, sqlRequest);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                })));
            }

            for (Future<?> future : futures) {
                try {
                    SequenceSQLResponse shardResult = (SequenceSQLResponse) future.get();

                    if (shardResult == null) {
                        throw new IllegalStateException("One of the shard result is null");
                    }
                    if (shardResult.getIsException()) {
                        throw new IllegalStateException("One of the shard met exception: " + shardResult.getExceptionMessage());
                    }

                    shardResults.add(shardResult);
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }

            SequenceSQLResponse finalResponse = new SequenceSQLResponse();
            int sum = (int) ValueIterators.sum(Iterators.transform(shardResults.iterator(), new Function<SequenceSQLResponse, Integer>() {
                @Nullable
                @Override
                public Integer apply(@Nullable SequenceSQLResponse input) {
                    return input.getResultCount();
                }

            }));

            int sqlID = ValueIterators.checkSame(Iterators.transform(shardResults.iterator(), new Function<SequenceSQLResponse, Integer>() {
                @Nullable
                @Override
                public Integer apply(@Nullable SequenceSQLResponse input) {
                    return input.getStepID();
                }

            }));

            //TODO: use this
            String cube = ValueIterators.checkSame(Iterators.transform(shardResults.iterator(), new Function<SequenceSQLResponse, String>() {
                @Nullable
                @Override
                public String apply(@Nullable SequenceSQLResponse input) {
                    return input.getCube();
                }

            }));

            finalResponse.setResultCount(sum);
            finalResponse.setSequenceID(sqlRequest.getSequenceID());
            finalResponse.setStepID(sqlID);
            //finalResponse.setCube(cube);
            finalResponse.setDuration(System.currentTimeMillis() - startTime);
            return finalResponse;
        } catch (Exception e) {
            logger.error("error", e);
            return

            createExceptionResponse(e);
        }

    }

    @RequestMapping(value = "/shardable_query_worker/execution", method = RequestMethod.POST)
    @ResponseBody
    public SequenceSQLResponse doShardableQuery(@RequestBody ShardedSequenceSQLRequest shardedSequenceSQLRequest) {
        try {
            if (shardedSequenceSQLRequest.getBackdoorToggles() == null) {
                shardedSequenceSQLRequest.setBackdoorToggles(Maps.<String, String> newHashMap());
            }
            shardedSequenceSQLRequest.getBackdoorToggles().put(BackdoorToggles.DEBUG_TOGGLE_SHARD_ASSIGNMENT, shardedSequenceSQLRequest.getWorkerCount() + "#" + shardedSequenceSQLRequest.getWorkerID());
            BackdoorToggles.setToggles(shardedSequenceSQLRequest.getBackdoorToggles());

            SQLResponse sqlResponse = null;
            String sql = shardedSequenceSQLRequest.getSql();

            if (!StringUtils.isEmpty(sql)) {
                String project = shardedSequenceSQLRequest.getProject();
                logger.info("Using project: " + project);
                logger.info("The original query:  " + sql);

                String serverMode = KylinConfig.getInstanceFromEnv().getServerMode();
                if (!(Constant.SERVER_MODE_QUERY.equals(serverMode.toLowerCase()) || Constant.SERVER_MODE_ALL.equals(serverMode.toLowerCase()))) {
                    throw new InternalErrorException("Query is not allowed in " + serverMode + " mode.");
                }

                if (!sql.toLowerCase().contains("select")) {
                    logger.debug("Directly return exception as not supported");
                    throw new InternalErrorException("Not Supported SQL.");
                }

                long startTime = System.currentTimeMillis();

                try {
                    sqlResponse = queryService.query(shardedSequenceSQLRequest);

                    sqlResponse.setDuration(System.currentTimeMillis() - startTime);
                    logger.info("Stats of SQL response: isException: {}, duration: {}, total scan count {}", //
                            String.valueOf(sqlResponse.getIsException()), String.valueOf(sqlResponse.getDuration()), String.valueOf(sqlResponse.getTotalScanCount()));

                    //TODO: auth
                    //checkQueryAuth(sqlResponse);

                } catch (Throwable e) { // calcite may throw AssertError
                    logger.error("Exception when execute sql", e);
                    String errMsg = QueryUtil.makeErrorMsgUserFriendly(e);

                    sqlResponse = new SQLResponse(null, null, 0, true, errMsg);
                }

                queryService.logQuery(shardedSequenceSQLRequest, sqlResponse);

                if (sqlResponse.getIsException())
                    throw new InternalErrorException(sqlResponse.getExceptionMessage());
            } else {
                logger.info("Updating the resultOpt only");
            }

            SequenceTopology topology = topologyManager.getTopology(shardedSequenceSQLRequest.getSequenceID(), shardedSequenceSQLRequest.getWorkerID());

            if (topology == null) {
                topologyManager.addTopology(shardedSequenceSQLRequest.getSequenceID(), shardedSequenceSQLRequest.getWorkerID());
                topology = topologyManager.getTopology(shardedSequenceSQLRequest.getSequenceID(), shardedSequenceSQLRequest.getWorkerID());
            }

            int stepID = shardedSequenceSQLRequest.getStepID();
            if (stepID == -1) {
                stepID = topology.addStep(shardedSequenceSQLRequest.getSql(), shardedSequenceSQLRequest.getSequenceOpt(), shardedSequenceSQLRequest.getResultOpt());
            } else {
                stepID = topology.updateStep(stepID, shardedSequenceSQLRequest.getSql(), shardedSequenceSQLRequest.getSequenceOpt(), shardedSequenceSQLRequest.getResultOpt());
            }
            int resultSize = topology.updateSQLNodeResult(stepID, sqlResponse);

            SequenceSQLResponse ret = new SequenceSQLResponse();
            if (sqlResponse != null) {
                //ret.setCube(sqlResponse.getCube());
            }
            ret.setResultCount(resultSize);
            ret.setSequenceID(shardedSequenceSQLRequest.getSequenceID());
            ret.setStepID(stepID);
            return ret;

        } catch (Exception e) {
            logger.error("error", e);
            return createExceptionResponse(e);
        } finally {
            BackdoorToggles.cleanToggles();
        }
    }

    @RequestMapping(value = "/sequence_sql/result/{sequenceID}", method = { RequestMethod.GET })
    @ResponseBody
    public SequenceSQLResponse getSequenceSQLResult(@PathVariable("sequenceID") final long sequenceID, @RequestHeader("Authorization") String basicAuthen) {
        try {

            long startTime = System.currentTimeMillis();

            final List<KAPRESTClient> workerClients = getWorkerClients(basicAuthen);
            List<SequenceSQLResponse> shardResults = Lists.newArrayList();
            List<Future<?>> futures = Lists.newArrayList();
            for (int i = 0; i < workerClients.size(); i++) {
                final int workerID = i;
                futures.add((executorService.submit(new Callable<SequenceSQLResponse>() {
                    @Override
                    public SequenceSQLResponse call() throws Exception {
                        try {
                            return workerClients.get(workerID).collectSequenceSQLResultFromWorker(workerID, sequenceID);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                })));
            }

            for (Future<?> future : futures) {
                try {
                    SequenceSQLResponse shardResult = (SequenceSQLResponse) future.get();

                    if (shardResult == null) {
                        throw new IllegalStateException("One of the shard result is null");
                    }
                    if (shardResult.getIsException()) {
                        throw new IllegalStateException("One of the shard met exception: " + shardResult.getExceptionMessage());
                    }

                    shardResults.add(shardResult);
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }

            SequenceSQLResponse finalResponse = new SequenceSQLResponse();
            List<List<String>> results = Lists.newArrayList();
            for (SequenceSQLResponse shardResult : shardResults) {
                results.addAll(shardResult.getResults());
            }

            finalResponse.setDuration(System.currentTimeMillis() - startTime);
            finalResponse.setResults(results);
            finalResponse.setResultCount(results.size());
            finalResponse.setSequenceID(Long.valueOf(sequenceID));
            return finalResponse;
        } catch (Exception e) {
            logger.error("error", e);
            return createExceptionResponse(e);
        }
    }

    @RequestMapping(value = "/shardable_query_worker/result/{sequenceID}/{workerID}", method = { RequestMethod.GET })
    @ResponseBody
    public SequenceSQLResponse getShardableQueryResult(@PathVariable("sequenceID") long sequenceID, @PathVariable("workerID") int workerID) {
        try {
            SequenceSQLResponse sequenceSQLResponse = new SequenceSQLResponse();
            logger.info("Trying to get shard result for {} on worker {} ", sequenceID, workerID);

            SequenceTopology topology = topologyManager.getTopology(sequenceID, workerID);

            if (topology == null) {
                throw new IllegalStateException("The sequence topology is not found, maybe expired?");
            }

            SequenceNodeOutput finalResult = topology.getSequeneFinalResult();

            if (finalResult != null) {
                sequenceSQLResponse.setResults(finalResult.getResults());
            } else {
                throw new IllegalStateException("The final result for current topology is not found!");
            }

            return sequenceSQLResponse;
        } catch (Exception e) {
            return createExceptionResponse(e);
        }
    }

    @RequestMapping(value = "/sequence_sql/topology/{sequenceID}", method = { RequestMethod.GET })
    @ResponseBody
    public List<String> getTopology(@PathVariable("sequenceID") final long sequenceID, @RequestHeader("Authorization") String basicAuthen) {

        final List<KAPRESTClient> workerClients = getWorkerClients(basicAuthen);
        List<String> shardResults = Lists.newArrayList();
        List<Future<?>> futures = Lists.newArrayList();
        for (int i = 0; i < workerClients.size(); i++) {
            final int workerID = i;
            futures.add((executorService.submit(new Callable<String>() {
                @Override
                public String call() throws Exception {
                    try {
                        return workerClients.get(workerID).collectStatsFromWorker(workerID, sequenceID);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            })));
        }

        for (Future<?> future : futures) {
            try {
                String shardResult = (String) future.get();
                shardResults.add(shardResult);
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        return shardResults;

    }

    @RequestMapping(value = "/shardable_query_worker/topology/{sequenceID}/{workerID}", method = { RequestMethod.GET })
    @ResponseBody
    public String getShardTopology(@PathVariable("sequenceID") final long sequenceID, @PathVariable("workerID") int workerID) {
        SequenceTopology topology = topologyManager.getTopology(sequenceID, workerID);
        if (topology == null) {
            return "";
        }

        return topology.toString();
    }

    private SequenceSQLResponse createExceptionResponse(Throwable e) {
        SequenceSQLResponse sequenceSQLResponse = new SequenceSQLResponse();
        sequenceSQLResponse.setIsException(true);
        sequenceSQLResponse.setExceptionMessage(Throwables.getStackTraceAsString(e));
        return sequenceSQLResponse;
    }

    private void checkQueryAuth(SQLResponse sqlResponse) throws AccessDeniedException {
        if (!sqlResponse.getIsException() && KylinConfig.getInstanceFromEnv().isQuerySecureEnabled()) {
            CubeInstance cubeInstance = this.queryService.getCubeManager().getCube(sqlResponse.getCube());
            queryService.checkAuthorization(cubeInstance.getName());
        }
    }

    public void setQueryService(QueryService queryService) {
        this.queryService = queryService;
    }

}
