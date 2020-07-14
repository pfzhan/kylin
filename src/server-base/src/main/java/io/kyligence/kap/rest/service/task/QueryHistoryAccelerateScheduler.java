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

package io.kyligence.kap.rest.service.task;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ExecutorServiceUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.optimization.FrequencyMap;
import io.kyligence.kap.metadata.epoch.EpochManager;
import io.kyligence.kap.metadata.favorite.AccelerateRuleUtil;
import io.kyligence.kap.metadata.favorite.QueryHistoryIdOffset;
import io.kyligence.kap.metadata.favorite.QueryHistoryIdOffsetManager;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.query.AccelerateRatioManager;
import io.kyligence.kap.metadata.query.NativeQueryRealization;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.metadata.query.QueryHistoryInfo;
import io.kyligence.kap.metadata.query.RDBMSQueryHistoryDAO;
import io.kyligence.kap.rest.service.RawRecService;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.val;

public class QueryHistoryAccelerateScheduler {

    private static final Logger logger = LoggerFactory.getLogger(QueryHistoryAccelerateScheduler.class);

    private ScheduledExecutorService queryHistoryAccelerateScheduler;
    private boolean hasStarted;
    @VisibleForTesting
    RDBMSQueryHistoryDAO queryHistoryDAO;
    AccelerateRuleUtil accelerateRuleUtil;
    RawRecService rawRecommendation;
    @Getter
    private String project;
    private long epochId;

    private static final Map<String, QueryHistoryAccelerateScheduler> INSTANCE_MAP = Maps.newConcurrentMap();

    public QueryHistoryAccelerateScheduler(String project) {
        this.project = project;
        queryHistoryDAO = RDBMSQueryHistoryDAO.getInstance(KylinConfig.getInstanceFromEnv());
        accelerateRuleUtil = new AccelerateRuleUtil();
        rawRecommendation = new RawRecService();
        logger.debug("New QueryHistoryAccelerateScheduler created by project {}", project);
    }

    public static QueryHistoryAccelerateScheduler getInstance(String project) {
        return INSTANCE_MAP.computeIfAbsent(project, QueryHistoryAccelerateScheduler::new);
    }

    public void init() {
        ProjectInstance projectInstance = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                .getProject(project);
        if (!KylinConfig.getInstanceFromEnv().isUTEnv()) {
            this.epochId = projectInstance.getEpoch().getEpochId();
        }

        queryHistoryAccelerateScheduler = Executors.newScheduledThreadPool(1,
                new NamedThreadFactory("QueryHistoryAccelerateWorker(project:" + project + ")"));
        queryHistoryAccelerateScheduler.scheduleWithFixedDelay(new QueryHistoryAccelerateRunner(), 0,
                KylinConfig.getInstanceFromEnv().getQueryHistoryAccelerateInterval(), TimeUnit.MINUTES);

        hasStarted = true;
        logger.info("Query history accelerate scheduler is started for [{}] ", project);
    }

    public Future scheduleImmediately() {
        return queryHistoryAccelerateScheduler.schedule(new QueryHistoryAccelerateRunner(), 10L, TimeUnit.SECONDS);
    }

    public boolean hasStarted() {
        return this.hasStarted;
    }

    private void shutdown() {
        logger.info("Shutting down QueryHistoryAccelerateScheduler ....");
        if (queryHistoryAccelerateScheduler != null) {
            ExecutorServiceUtil.forceShutdown(queryHistoryAccelerateScheduler);
        }
    }

    public static synchronized void shutdownByProject(String project) {
        val instance = getInstanceByProject(project);
        if (instance != null) {
            INSTANCE_MAP.remove(project);
            instance.shutdown();
        }
    }

    private static synchronized QueryHistoryAccelerateScheduler getInstanceByProject(String project) {
        return INSTANCE_MAP.get(project);
    }

    @NoArgsConstructor
    public class QueryHistoryAccelerateRunner implements Runnable {

        @Override
        public void run() {
            if (NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(project).isExpertMode()) {
                return;
            }
            if (!KylinConfig.getInstanceFromEnv().isUTEnv()
                    && !EpochManager.getInstance(KylinConfig.getInstanceFromEnv()).checkEpochId(epochId, project)) {
                shutdownByProject(project);
                return;
            }

            QueryHistoryIdOffset queryHistoryIdOffset = QueryHistoryIdOffsetManager
                    .getInstance(KylinConfig.getInstanceFromEnv(), project).get();

            int accelerateBatchSize = KylinConfig.getInstanceFromEnv().getQueryHistoryAccelerateBatchSize();
            int accelerateMaxSize = KylinConfig.getInstanceFromEnv().getQueryHistoryAccelerateMaxSize();
            int acceleratedCounts = 0;

            while (true) {
                List<QueryHistory> queryHistories = queryHistoryDAO.getQueryHistoriesById(
                        queryHistoryIdOffset.getQueryHistoryIdOffset(), accelerateBatchSize, project);
                acceleratedCounts = acceleratedCounts + queryHistories.size();
                accelerateAndUpdateMetadata(queryHistories);
                if (queryHistories.size() < accelerateBatchSize || acceleratedCounts >= accelerateMaxSize) {
                    break;
                }
            }
        }

        private void accelerateAndUpdateMetadata(List<QueryHistory> queryHistories) {
            if (CollectionUtils.isEmpty(queryHistories)) {
                return;
            }
            int numOfQueryHitIndex = 0;
            int overallQueryNum = 0;
            long maxId = 0;

            Map<String, Long> modelsLastQueryTime = Maps.newHashMap();
            val dfHitCountMap = collectDataflowHitCount(queryHistories);
            for (QueryHistory queryHistory : queryHistories) {
                overallQueryNum++;
                collectModelLastQueryTime(queryHistory, modelsLastQueryTime);

                String engineType = queryHistory.getEngineType();
                if (Objects.nonNull(engineType) && engineType.equals(QueryHistory.EngineType.NATIVE.name())) {
                    numOfQueryHitIndex++;
                }
                if (queryHistory.getId() > maxId) {
                    maxId = queryHistory.getId();
                }
            }

            // accelerate
            List<Pair<Long, QueryHistoryInfo>> queryHistoryInfos = Lists.newArrayList();
            List<QueryHistory> matchedCandidate = accelerateRuleUtil.findMatchedCandidate(project, queryHistories,
                    queryHistoryInfos);
            updateQueryHistoryInfo(queryHistoryInfos);
            rawRecommendation.generateRawRecommendations(project, matchedCandidate);

            // update metadata
            updateMetadata(numOfQueryHitIndex, overallQueryNum, dfHitCountMap, modelsLastQueryTime, maxId);
        }

        private void updateQueryHistoryInfo(List<Pair<Long, QueryHistoryInfo>> queryHistoryInfos) {
            List<Object[]> batchArgs = Lists.newArrayList();
            queryHistoryInfos.forEach(pair -> {
                String qhInfo = "";
                try {
                    qhInfo = JsonUtil.writeValueAsString(pair.getSecond());
                } catch (JsonProcessingException e) {
                    logger.error("Fail to parse query history info", e);
                }
                batchArgs.add(new Object[] { qhInfo.getBytes(), pair.getFirst(), });
            });
            queryHistoryDAO.batchUpdataQueryHistorieInfo(batchArgs);
        }

        private void updateMetadata(int numOfQueryHitIndex, int overallQueryNum,
                Map<String, DataflowHitCount> dfHitCountMap, Map<String, Long> modelsLastQueryTime, Long maxId) {
            EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                KylinConfig config = KylinConfig.getInstanceFromEnv();

                // update model usage
                incQueryHitCount(dfHitCountMap, project);

                // update model last query time
                updateLastQueryTime(modelsLastQueryTime, project);

                // update accelerate ratio
                if (numOfQueryHitIndex != 0 || overallQueryNum != 0) {
                    AccelerateRatioManager accelerateRatioManager = AccelerateRatioManager.getInstance(config, project);
                    accelerateRatioManager.increment(numOfQueryHitIndex, overallQueryNum);
                }

                // update id offset
                QueryHistoryIdOffset queryHistoryIdOffset = QueryHistoryIdOffsetManager
                        .getInstance(KylinConfig.getInstanceFromEnv(), project).get();
                queryHistoryIdOffset.setQueryHistoryIdOffset(maxId);
                QueryHistoryIdOffsetManager.getInstance(config, project).save(queryHistoryIdOffset);
                return 0;
            }, project);
        }

        private Map<String, DataflowHitCount> collectDataflowHitCount(List<QueryHistory> queryHistories) {
            val dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            val result = Maps.<String, DataflowHitCount> newHashMap();
            for (QueryHistory queryHistory : queryHistories) {
                val realizations = queryHistory.transformRealizations();
                if (CollectionUtils.isEmpty(realizations)) {
                    continue;
                }
                for (val realization : realizations) {
                    if (dfManager.getDataflow(realization.getModelId()) == null) {
                        continue;
                    }
                    result.computeIfAbsent(realization.getModelId(), k -> new DataflowHitCount());
                    result.get(realization.getModelId()).dataflowHit += 1;
                    val layoutHits = result.get(realization.getModelId()).getLayoutHits();
                    layoutHits.computeIfAbsent(realization.getLayoutId(), k -> new FrequencyMap());
                    layoutHits.get(realization.getLayoutId()).incFrequency(queryHistory.getQueryTime());
                }
            }
            return result;
        }

        private void collectModelLastQueryTime(QueryHistory queryHistory, Map<String, Long> modelsLastQueryTime) {
            List<NativeQueryRealization> realizations = queryHistory.transformRealizations();
            long queryTime = queryHistory.getQueryTime();
            for (NativeQueryRealization realization : realizations) {
                String modelId = realization.getModelId();
                modelsLastQueryTime.put(modelId, queryTime);
            }
        }

        private void incQueryHitCount(Map<String, DataflowHitCount> dfHitCountMap, String project) {
            val dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            for (val entry : dfHitCountMap.entrySet()) {
                if (dfManager.getDataflow(entry.getKey()) == null) {
                    continue;
                }
                val layoutHitCount = entry.getValue().getLayoutHits();
                dfManager.updateDataflow(entry.getKey(), copyForWrite -> {
                    copyForWrite.setQueryHitCount(copyForWrite.getQueryHitCount() + entry.getValue().getDataflowHit());
                    for (Map.Entry<Long, FrequencyMap> layoutHitEntry : layoutHitCount.entrySet()) {
                        copyForWrite.getLayoutHitCount().merge(layoutHitEntry.getKey(), layoutHitEntry.getValue(),
                                FrequencyMap::merge);
                    }
                });
            }
        }

        private void updateLastQueryTime(Map<String, Long> modelsLastQueryTime, String project) {
            val dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            List<NDataflow> dataflows = dfManager.listAllDataflows();
            dataflows.stream().filter(dataflow -> modelsLastQueryTime.containsKey(dataflow.getId()))
                    .forEach(dataflow -> dfManager.updateDataflow(dataflow.getId(),
                            copyForWrite -> copyForWrite.setLastQueryTime(modelsLastQueryTime.get(dataflow.getId()))));
        }

    }

    @Data
    static class DataflowHitCount {

        Map<Long, FrequencyMap> layoutHits = Maps.newHashMap();

        int dataflowHit;
    }
}
