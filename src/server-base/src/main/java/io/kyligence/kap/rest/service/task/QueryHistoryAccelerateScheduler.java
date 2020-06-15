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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ExecutorServiceUtil;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.optimization.FrequencyMap;
import io.kyligence.kap.metadata.favorite.AccelerateRuleUtil;
import io.kyligence.kap.metadata.favorite.QueryHistoryIdOffset;
import io.kyligence.kap.metadata.favorite.QueryHistoryIdOffsetManager;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.metadata.query.AccelerateRatioManager;
import io.kyligence.kap.metadata.query.NativeQueryRealization;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.metadata.query.RDBMSQueryHistoryDAO;
import io.kyligence.kap.rest.service.RawRecommendationService;
import lombok.Data;
import lombok.val;

public class QueryHistoryAccelerateScheduler {

    private static final Logger logger = LoggerFactory.getLogger(QueryHistoryAccelerateScheduler.class);

    private ScheduledExecutorService queryHistoryAccelerateScheduler;
    private ExecutorService projectQueryHistoryAccelerateService;
    private boolean hasStarted;

    public QueryHistoryAccelerateScheduler() {
        logger.debug("New QueryHistoryAccelerateScheduler created");
    }

    public static QueryHistoryAccelerateScheduler getInstance() {
        return new QueryHistoryAccelerateScheduler();
    }

    public void init() {
        queryHistoryAccelerateScheduler = Executors.newScheduledThreadPool(1,
                new NamedThreadFactory("QueryHistoryAccelerateWorker"));
        queryHistoryAccelerateScheduler.scheduleWithFixedDelay(new QueryHistoryAccelerateRunner(), 0,
                KylinConfig.getInstanceFromEnv().getQueryHistoryAccelerateInterval(), TimeUnit.SECONDS);

        projectQueryHistoryAccelerateService = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(), new NamedThreadFactory("ProjectQueryHistoryAccelerateWorker"));
        hasStarted = true;
        logger.info("Query history accelerate scheduler is started");
    }

    public Future scheduleImmediately() {
        ScheduledFuture<?> future = queryHistoryAccelerateScheduler.schedule(new QueryHistoryAccelerateRunner(), 10L,
                TimeUnit.SECONDS);
        return future;
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

    public class QueryHistoryAccelerateRunner implements Runnable {

        public QueryHistoryAccelerateRunner() {

        }

        @Override
        public void run() {
            QueryHistoryIdOffset queryHistoryIdOffset = QueryHistoryIdOffsetManager
                    .getInstance(KylinConfig.getInstanceFromEnv()).get();
            RDBMSQueryHistoryDAO queryHistoryDAO = RDBMSQueryHistoryDAO.getInstance(KylinConfig.getInstanceFromEnv());

            int accelerateBatchSize = KylinConfig.getInstanceFromEnv().getQueryHistoryAccelerateBatchSize();
            int accelerateMaxSize = KylinConfig.getInstanceFromEnv().getQueryHistoryAccelerateMaxSize();
            int acceleratedCounts = 0;

            while (true) {
                List<QueryHistory> queryHistories = queryHistoryDAO
                        .getQueryHistoriesById(queryHistoryIdOffset.getQueryHistoryIdOffset(), accelerateBatchSize);
                acceleratedCounts = acceleratedCounts + queryHistories.size();
                accelerateAndUpdateMetadata(queryHistories);
                if (queryHistories.size() < accelerateBatchSize || acceleratedCounts >= accelerateMaxSize) {
                    break;
                }
            }
        }

        private void accelerateAndUpdateMetadata(List<QueryHistory> queryHistories) {
            ArrayListMultimap<String, QueryHistory> queryHistoryMap = ArrayListMultimap.create();
            for (QueryHistory queryHistory : queryHistories) {
                queryHistoryMap.put(queryHistory.getProjectName(), queryHistory);
            }

            for (String project : queryHistoryMap.keySet()) {
                projectQueryHistoryAccelerateService.execute(() -> {
                    accelerateAndUpdateMetadata(project, queryHistoryMap.get(project));
                });
            }
        }

        private void accelerateAndUpdateMetadata(String project, List<QueryHistory> queryHistories) {
            if (CollectionUtils.isEmpty(queryHistories)) {
                return;
            }
            int numOfQueryHitIndex = 0;
            int overallQueryNum = 0;

            Map<String, Long> modelsLastQueryTime = Maps.newHashMap();
            val dfHitCountMap = collectDataflowHitCount(queryHistories, project);
            for (QueryHistory queryHistory : queryHistories) {
                overallQueryNum++;
                collectModelLastQueryTime(queryHistory, modelsLastQueryTime);

                if (queryHistory.getEngineType().equals(QueryHistory.EngineType.NATIVE.name())) {
                    numOfQueryHitIndex++;
                }
            }

            updateMetadata(numOfQueryHitIndex, overallQueryNum, dfHitCountMap, modelsLastQueryTime, queryHistories,
                    project);

            // accelerate
            AccelerateRuleUtil accelerateRuleUtil = new AccelerateRuleUtil();
            List<QueryHistory> matchedCandidate = accelerateRuleUtil.findMatchedCandidate(project, queryHistories);
            RawRecommendationService rawRecommendation = new RawRecommendationService();
            rawRecommendation.generateRawRecommendations(project, matchedCandidate);
        }

        private void updateMetadata(int numOfQueryHitIndex, int overallQueryNum,
                Map<String, DataflowHitCount> dfHitCountMap, Map<String, Long> modelsLastQueryTime,
                List<QueryHistory> queryHistories, String project) {
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
                        .getInstance(KylinConfig.getInstanceFromEnv()).get();
                long idOffset = queryHistoryIdOffset.getQueryHistoryIdOffset();
                queryHistoryIdOffset.setQueryHistoryIdOffset(idOffset + queryHistories.size());
                QueryHistoryIdOffsetManager.getInstance(config).save(queryHistoryIdOffset);
                return 0;
            }, project);
        }

        private Map<String, DataflowHitCount> collectDataflowHitCount(List<QueryHistory> queryHistories,
                String project) {
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
