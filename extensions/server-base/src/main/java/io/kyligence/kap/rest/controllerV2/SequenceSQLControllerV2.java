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

package io.kyligence.kap.rest.controllerV2;

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
import org.apache.kylin.query.util.QueryUtil;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.service.QueryServiceV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.common.util.ValueIterators;
import io.kyligence.kap.rest.client.KAPRESTClient;
import io.kyligence.kap.rest.msg.KapMessage;
import io.kyligence.kap.rest.msg.KapMsgPicker;
import io.kyligence.kap.rest.request.SequenceSQLRequest;
import io.kyligence.kap.rest.request.ShardedSequenceSQLRequest;
import io.kyligence.kap.rest.response.SequenceSQLResponse;
import io.kyligence.kap.rest.sequencesql.DiskResultCache;
import io.kyligence.kap.rest.sequencesql.SequenceNodeOutput;
import io.kyligence.kap.rest.sequencesql.SequenceOpt;
import io.kyligence.kap.rest.sequencesql.topology.SequenceTopology;
import io.kyligence.kap.rest.sequencesql.topology.SequenceTopologyManager;

@Controller
public class SequenceSQLControllerV2 extends BasicController {

    private static final Logger logger = LoggerFactory.getLogger(SequenceSQLControllerV2.class);

    @Autowired
    @Qualifier("queryServiceV2")
    private QueryServiceV2 queryServiceV2;

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

    @RequestMapping(value = "/sequence_sql/execution", method = RequestMethod.POST, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse doSequenceSql(@RequestHeader("Accept-Language") String lang, @RequestBody final SequenceSQLRequest sqlRequest, @RequestHeader("Authorization") String basicAuthen) throws ExecutionException, InterruptedException {
        KapMsgPicker.setMsg(lang);
        KapMessage msg = KapMsgPicker.getMsg();

        long startTime = System.currentTimeMillis();
        if (sqlRequest.getSequenceID() == -1) {
            throw new BadRequestException(msg.getUNIQUE_SEQ_ID_REQUIRED());
        }

        if (sqlRequest.getSequenceOpt() != SequenceOpt.UPDATE) {
            if (sqlRequest.getStepID() != -1) {
                throw new BadRequestException(msg.getSTEPID_NOT_DEFAULT());
            }

            if (sqlRequest.getSequenceOpt() == SequenceOpt.APPEND) {
                if (sqlRequest.getSql() == null || sqlRequest.getResultOpt() == null) {
                    throw new BadRequestException(msg.getSQL_AND_RESULT_OPT_REQUIRED());
                }
            }

            if (sqlRequest.getSequenceOpt() == SequenceOpt.INIT) {
                if (sqlRequest.getSql() == null)
                    throw new BadRequestException(msg.getSQL_REQUIRED());
            }
        }

        if (sqlRequest.getSequenceOpt() == SequenceOpt.UPDATE) {
            if (sqlRequest.getStepID() == -1) {
                throw new BadRequestException(msg.getEXISTING_STEPID_REQUIRED());
            }
            if (sqlRequest.getStepID() == 0 && sqlRequest.getResultOpt() != null) {
                throw new BadRequestException(msg.getUPDATE_STEP0_RESULT_OPT());
            }
            if (sqlRequest.getSql() == null && sqlRequest.getResultOpt() == null) {
                throw new BadRequestException(msg.getSQL_OR_RESULT_OPT_REQUIRED());
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
                        throw new InternalErrorException(e);
                    }
                }
            })));
        }

        for (Future<?> future : futures) {
            SequenceSQLResponse shardResult = (SequenceSQLResponse) future.get();

            if (shardResult == null) {
                throw new BadRequestException(msg.getONE_SHARED_RESULT_NULL());
            }
            if (shardResult.getIsException()) {
                throw new BadRequestException(String.format(msg.getONE_SHARED_EXCEPTION(), shardResult.getExceptionMessage()));
            }

            shardResults.add(shardResult);
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
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, finalResponse, "");
    }

    @RequestMapping(value = "/shardable_query_worker/execution", method = RequestMethod.POST, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse doShardableQuery(@RequestHeader("Accept-Language") String lang, @RequestBody ShardedSequenceSQLRequest shardedSequenceSQLRequest) {
        KapMsgPicker.setMsg(lang);
        KapMessage msg = KapMsgPicker.getMsg();

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
                    throw new BadRequestException(String.format(msg.getQUERY_NOT_ALLOWED(), serverMode));
                }

                if (!sql.toLowerCase().contains("select")) {
                    logger.debug("Directly return exception as not supported");
                    throw new BadRequestException(msg.getNOT_SUPPORTED_SQL());
                }

                long startTime = System.currentTimeMillis();

                try {
                    sqlResponse = queryServiceV2.query(shardedSequenceSQLRequest);

                    sqlResponse.setDuration(System.currentTimeMillis() - startTime);
                    logger.info("Stats of SQL response: isException: {}, duration: {}, total scan count {}", //
                            String.valueOf(sqlResponse.getIsException()), String.valueOf(sqlResponse.getDuration()), String.valueOf(sqlResponse.getTotalScanCount()));

                    //TODO: auth
                    //checkQueryAuth(sqlResponse);

                } catch (Throwable e) { // calcite may throw AssertError
                    logger.error("Exception when execute sql", e);
                    String errMsg = QueryUtil.makeErrorMsgUserFriendly(e);
                    throw new BadRequestException(errMsg);
                }

                queryServiceV2.logQuery(shardedSequenceSQLRequest, sqlResponse);

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
            return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, ret, "");

        } finally {
            BackdoorToggles.cleanToggles();
        }
    }

    @RequestMapping(value = "/sequence_sql/result/{sequenceID}", method = { RequestMethod.GET }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getSequenceSQLResult(@RequestHeader("Accept-Language") String lang, @PathVariable("sequenceID") final long sequenceID, @RequestHeader("Authorization") String basicAuthen) throws ExecutionException, InterruptedException {
        KapMsgPicker.setMsg(lang);
        KapMessage msg = KapMsgPicker.getMsg();

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
                        throw new InternalErrorException(e);
                    }
                }
            })));
        }

        for (Future<?> future : futures) {
            SequenceSQLResponse shardResult = (SequenceSQLResponse) future.get();

            if (shardResult == null) {
                throw new BadRequestException(msg.getONE_SHARED_RESULT_NULL());
            }
            if (shardResult.getIsException()) {
                throw new BadRequestException(String.format(msg.getONE_SHARED_EXCEPTION(), shardResult.getExceptionMessage()));
            }

            shardResults.add(shardResult);
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
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, finalResponse, "");
    }

    @RequestMapping(value = "/shardable_query_worker/result/{sequenceID}/{workerID}", method = { RequestMethod.GET }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getShardableQueryResult(@RequestHeader("Accept-Language") String lang, @PathVariable("sequenceID") long sequenceID, @PathVariable("workerID") int workerID) {
        KapMsgPicker.setMsg(lang);
        KapMessage msg = KapMsgPicker.getMsg();

        SequenceSQLResponse sequenceSQLResponse = new SequenceSQLResponse();
        logger.info("Trying to get shard result for {} on worker {} ", sequenceID, workerID);

        SequenceTopology topology = topologyManager.getTopology(sequenceID, workerID);

        if (topology == null) {
            throw new BadRequestException(msg.getSEQ_TOPOLOGY_NOT_FOUND());
        }

        SequenceNodeOutput finalResult = topology.getSequeneFinalResult();

        if (finalResult != null) {
            sequenceSQLResponse.setResults(finalResult.getResults());
        } else {
            throw new BadRequestException(msg.getTOPOLOGY_FINAL_RESULT_NOT_FOUND());
        }

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, sequenceSQLResponse, "");
    }

    @RequestMapping(value = "/sequence_sql/topology/{sequenceID}", method = { RequestMethod.GET }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getTopology(@RequestHeader("Accept-Language") String lang, @PathVariable("sequenceID") final long sequenceID, @RequestHeader("Authorization") String basicAuthen) throws ExecutionException, InterruptedException {
        KapMsgPicker.setMsg(lang);

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
                        throw new InternalErrorException(e);
                    }
                }
            })));
        }

        for (Future<?> future : futures) {
            String shardResult = (String) future.get();
            shardResults.add(shardResult);
        }

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, shardResults, "");
    }

    @RequestMapping(value = "/shardable_query_worker/topology/{sequenceID}/{workerID}", method = { RequestMethod.GET }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getShardTopology(@RequestHeader("Accept-Language") String lang, @PathVariable("sequenceID") final long sequenceID, @PathVariable("workerID") int workerID) {
        KapMsgPicker.setMsg(lang);
        KapMessage msg = KapMsgPicker.getMsg();

        SequenceTopology topology = topologyManager.getTopology(sequenceID, workerID);
        if (topology == null) {
            return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, "", "");
        }

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, topology.toString(), "");
    }
    //
    //    private void checkQueryAuth(SQLResponse sqlResponse) throws AccessDeniedException {
    //        if (!sqlResponse.getIsException() && KylinConfig.getInstanceFromEnv().isQuerySecureEnabled()) {
    //            CubeInstance cubeInstance = this.queryServiceV2.getCubeManager().getCube(sqlResponse.getCube());
    //            queryServiceV2.checkAuthorization(cubeInstance.getName());
    //        }
    //    }
    //
    //    public void setQueryService(QueryServiceV2 queryServiceV2) {
    //        this.queryServiceV2 = queryServiceV2;
    //    }
}
