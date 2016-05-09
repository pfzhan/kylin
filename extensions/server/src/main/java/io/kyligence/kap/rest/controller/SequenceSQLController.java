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
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import javax.annotation.Nullable;
import javax.annotation.PostConstruct;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.common.util.LoggableCachedThreadPool;
import org.apache.kylin.common.util.NumberIterators;
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
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.rest.client.KAPRESTClient;
import io.kyligence.kap.rest.request.SequenceSQLRequest;
import io.kyligence.kap.rest.request.ShardedSequenceSQLRequest;
import io.kyligence.kap.rest.response.SequenceSQLResponse;

@Controller
public class SequenceSQLController extends BasicController {

    private static final Logger logger = LoggerFactory.getLogger(SequenceSQLController.class);

    public static final String SEQUENCE_QUERY_CACHE = "SequenceSQLResults";

    @Autowired
    private QueryService queryService;

    @Autowired
    private CacheManager cacheManager;

    private static ExecutorService executorService = new LoggableCachedThreadPool();
    private static Random random = new Random();

    @PostConstruct
    public void init() throws IOException {
        Preconditions.checkNotNull(cacheManager, "cacheManager is not injected yet");
    }

    @RequestMapping(value = "/sequence_sql/execution", method = RequestMethod.POST)
    @ResponseBody
    public SequenceSQLResponse doSequenceSql(@RequestBody final SequenceSQLRequest sqlRequest) {
        if (sqlRequest.getSessionID() == 0) {
            sqlRequest.setSessionID(random.nextLong());
        }

        final String[] servers = KylinConfig.getInstanceFromEnv().getRestServers();
        final List<KAPRESTClient> restClient = Lists.newArrayList();
        final int workerCount = servers.length * KylinConfig.getInstanceFromEnv().getWorkersPerServer();
        for (int i = 0; i < workerCount; i++) {
            logger.info("worker {} : {}", i, servers[i % servers.length]);
            restClient.add(new KAPRESTClient(servers[i % servers.length]));
        }

        List<SequenceSQLResponse> shardResults = Lists.newArrayList();
        List<Future<?>> futures = Lists.newArrayList();
        for (int i = 0; i < workerCount; i++) {
            final int workerID = i;
            futures.add((executorService.submit(new Callable<SequenceSQLResponse>() {
                @Override
                public SequenceSQLResponse call() throws Exception {
                    try {
                        return restClient.get(workerID).dispatchSequenceSQLExecutionToWorker(workerCount, workerID, sqlRequest);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            })));
        }

        for (Future<?> future : futures) {
            try {
                shardResults.add((SequenceSQLResponse) future.get());
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        SequenceSQLResponse finalResponse = new SequenceSQLResponse();
        long sum = NumberIterators.sum(Iterators.transform(shardResults.iterator(), new Function<SequenceSQLResponse, Integer>() {

            @Nullable
            @Override
            public Integer apply(@Nullable SequenceSQLResponse input) {
                return input.getResultCount();
            }

        }));

        if (sum > Integer.MAX_VALUE) {
            throw new RuntimeException("result sum exceeds Integer Max: " + sum);
        }
        finalResponse.setResultCount((int) sum);
        finalResponse.setSessionID(sqlRequest.getSessionID());
        return finalResponse;

    }

    @RequestMapping(value = "/sequence_sql/result/{sessionID}", method = { RequestMethod.GET })
    @ResponseBody
    public SequenceSQLResponse getSequenceSQLResult(@PathVariable final String sessionID) {

        final String[] servers = KylinConfig.getInstanceFromEnv().getRestServers();
        final List<KAPRESTClient> restClient = Lists.newArrayList();
        final int workerCount = servers.length * KylinConfig.getInstanceFromEnv().getWorkersPerServer();
        for (int i = 0; i < workerCount; i++) {
            logger.info("worker {} : {}", i, servers[i % servers.length]);
            restClient.add(new KAPRESTClient(servers[i % servers.length]));
        }


        List<SequenceSQLResponse> shardResults = Lists.newArrayList();
        List<Future<?>> futures = Lists.newArrayList();
        for (int i = 0; i < workerCount; i++) {
            final int workerID = i;
            futures.add((executorService.submit(new Callable<SequenceSQLResponse>() {
                @Override
                public SequenceSQLResponse call() throws Exception {
                    try {
                        return restClient.get(workerID).collectSequenceSQLResultFromWorker(workerID, sessionID);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            })));
        }

        for (Future<?> future : futures) {
            try {
                shardResults.add((SequenceSQLResponse) future.get());
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
        
        SequenceSQLResponse finalResponse = new SequenceSQLResponse();
        List<List<String>> results = Lists.newArrayList();
        for (SequenceSQLResponse shardResult : shardResults) {
            if (shardResult != null) {
                results.addAll(shardResult.getResults());
            }
        }

        finalResponse.setResults(results);
        finalResponse.setSessionID(Long.valueOf(sessionID));
        return finalResponse;
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

            String sql = shardedSequenceSQLRequest.getSql();
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

            SQLResponse sqlResponse = null;
            try {
                sqlResponse = queryService.query(shardedSequenceSQLRequest);

                sqlResponse.setDuration(System.currentTimeMillis() - startTime);
                logger.info("Stats of SQL response: isException: {}, duration: {}, total scan count {}", //
                        new String[] { String.valueOf(sqlResponse.getIsException()), String.valueOf(sqlResponse.getDuration()), String.valueOf(sqlResponse.getTotalScanCount()) });

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

            SequenceSQLResult sequenceSQLResult = persistIntermediateResult(shardedSequenceSQLRequest, sqlResponse, shardedSequenceSQLRequest.getSessionID(), shardedSequenceSQLRequest.getOpt());

            SequenceSQLResponse ret = new SequenceSQLResponse();
            ret.setResultCount(sequenceSQLResult.size());
            ret.setSessionID(shardedSequenceSQLRequest.getSessionID());
            return ret;

        } finally {
            BackdoorToggles.cleanToggles();
        }
    }

    @RequestMapping(value = "/shardable_query_worker/result/{sessionID}/{workerID}", method = { RequestMethod.GET })
    @ResponseBody
    public SequenceSQLResponse getShardableQueryResult(@PathVariable String sessionID, @PathVariable String workerID) {
        Cache cache = cacheManager.getCache(SEQUENCE_QUERY_CACHE);
        SequenceSQLResponse sequenceSQLResponse = new SequenceSQLResponse();
        String s = generateShardResultKey(sessionID, workerID);
        logger.info("Trying to get shard result for key: " + s);
        Element element = cache.get(s);

        if (element != null) {
            SequenceSQLResult sequenceSQLResult = (SequenceSQLResult) element.getObjectValue();
            sequenceSQLResponse.setResults(sequenceSQLResult.results);
        } else {
            logger.info("shard result missing! Existing keys:" + cache.getKeys());
            //TODO: rerun to get the shard result
        }

        return sequenceSQLResponse;
    }

    private SequenceSQLResult persistIntermediateResult(ShardedSequenceSQLRequest shardedSequenceSQLRequest, SQLResponse sqlResponse, long sessionID, SequenceSQLRequest.OptWithLastResult opt) {

        SequenceSQLResult current = new SequenceSQLResult(sqlResponse);

        long startTime = System.currentTimeMillis();
        Cache cache = cacheManager.getCache(SEQUENCE_QUERY_CACHE);
        if (opt == SequenceSQLRequest.OptWithLastResult.NONE) {
        } else if (opt == SequenceSQLRequest.OptWithLastResult.INTERSECT) {
            current.intersect((SequenceSQLResult) cache.get(sessionID).getObjectValue());
        } else if (opt == SequenceSQLRequest.OptWithLastResult.UNION) {
            current.union((SequenceSQLResult) cache.get(sessionID).getObjectValue());
        } else if (opt == SequenceSQLRequest.OptWithLastResult.FORWARD_EXCEPT) {
            current.forwardExcept((SequenceSQLResult) cache.get(sessionID).getObjectValue());
        } else if (opt == SequenceSQLRequest.OptWithLastResult.BACKWARD_EXCEPT) {
            current.backwardExcept((SequenceSQLResult) cache.get(sessionID).getObjectValue());
        } else {
            throw new RuntimeException("Unknown OPT:" + opt);
        }
        cache.put(new Element(generateShardResultKey(String.valueOf(sessionID), String.valueOf(shardedSequenceSQLRequest.getWorkerID())), current));
        logger.info("Time to persist:" + (System.currentTimeMillis() - startTime) + " with OPT being " + opt);
        return current;
    }

    private String generateShardResultKey(String sessionID, String workerID) {
        return sessionID + "#" + workerID;
    }

    private void checkQueryAuth(SQLResponse sqlResponse) throws AccessDeniedException {
        if (!sqlResponse.getIsException() && KylinConfig.getInstanceFromEnv().isQuerySecureEnabled()) {
            CubeInstance cubeInstance = this.queryService.getCubeManager().getCube(sqlResponse.getCube());
            queryService.checkAuthorization(cubeInstance);
        }
    }

    public void setQueryService(QueryService queryService) {
        this.queryService = queryService;
    }

    public void setCacheManager(CacheManager cacheManager) {
        this.cacheManager = cacheManager;
    }

}
