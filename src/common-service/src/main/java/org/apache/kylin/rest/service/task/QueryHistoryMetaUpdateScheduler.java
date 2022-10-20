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

package org.apache.kylin.rest.service.task;

import static org.apache.kylin.metadata.favorite.QueryHistoryIdOffset.OffsetType.META;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.ExecutorServiceUtil;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.optimization.FrequencyMap;
import org.apache.kylin.metadata.epoch.EpochManager;
import org.apache.kylin.metadata.favorite.AbstractAsyncTask;
import org.apache.kylin.metadata.favorite.AccelerateRuleUtil;
import org.apache.kylin.metadata.favorite.AsyncAccelerationTask;
import org.apache.kylin.metadata.favorite.AsyncTaskManager;
import org.apache.kylin.metadata.favorite.QueryHistoryIdOffset;
import org.apache.kylin.metadata.favorite.QueryHistoryIdOffsetManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.query.NativeQueryRealization;
import org.apache.kylin.metadata.query.QueryHistory;
import org.apache.kylin.metadata.query.RDBMSQueryHistoryDAO;
import org.apache.kylin.rest.service.IUserGroupService;
import org.apache.kylin.rest.util.SpringContext;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;

import lombok.Data;
import lombok.Getter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QueryHistoryMetaUpdateScheduler {
    private ScheduledExecutorService taskScheduler;
    private boolean hasStarted;
    @VisibleForTesting
    RDBMSQueryHistoryDAO queryHistoryDAO;
    AccelerateRuleUtil accelerateRuleUtil;
    @Getter
    private final String project;
    private long epochId;
    private IUserGroupService userGroupService;

    private final QueryHistoryMetaUpdateRunner queryHistoryMetaUpdateRunner;

    private static final Map<String, QueryHistoryMetaUpdateScheduler> INSTANCE_MAP = Maps.newConcurrentMap();

    public QueryHistoryMetaUpdateScheduler(String project) {
        this.project = project;
        queryHistoryDAO = RDBMSQueryHistoryDAO.getInstance();
        accelerateRuleUtil = new AccelerateRuleUtil();
        if (userGroupService == null && SpringContext.getApplicationContext() != null) {
            userGroupService = (IUserGroupService) SpringContext.getApplicationContext().getBean("userGroupService");
        }
        queryHistoryMetaUpdateRunner = new QueryHistoryMetaUpdateRunner();
        log.debug("New QueryHistoryMetaUpdateScheduler created by project {}", project);
    }

    public static QueryHistoryMetaUpdateScheduler getInstance(String project) {
        return INSTANCE_MAP.computeIfAbsent(project, QueryHistoryMetaUpdateScheduler::new);
    }

    public void init() {
        ProjectInstance projectInstance = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                .getProject(project);

        EpochManager epochManager = EpochManager.getInstance();
        if (!KylinConfig.getInstanceFromEnv().isUTEnv()) {
            this.epochId = epochManager.getEpoch(projectInstance.getName()).getEpochId();
        }

        taskScheduler = Executors.newScheduledThreadPool(1,
                new NamedThreadFactory("QueryHistoryMetaUpdateWorker(project:" + project + ")"));
        taskScheduler.scheduleWithFixedDelay(queryHistoryMetaUpdateRunner, 0,
                KylinConfig.getInstanceFromEnv().getQueryHistoryStatMetaUpdateInterval(), TimeUnit.MINUTES);

        hasStarted = true;
        AsyncTaskManager.resetAccelerationTagMap(project);
        log.info("Query history task scheduler is started for [{}] ", project);
    }

    public Future scheduleImmediately(QueryHistoryTask runner) {
        return taskScheduler.schedule(runner, 10L, TimeUnit.SECONDS);
    }

    public boolean hasStarted() {
        return this.hasStarted;
    }

    private void shutdown() {
        log.info("Shutting down QueryHistoryAccelerateScheduler for [{}] ....", project);
        if (taskScheduler != null) {
            ExecutorServiceUtil.forceShutdown(taskScheduler);
        }
    }

    public static synchronized void shutdownByProject(String project) {
        val instance = getInstanceByProject(project);
        if (instance != null) {
            INSTANCE_MAP.remove(project);
            instance.shutdown();
        }
    }

    public boolean isInterruptByUser() {
        AsyncTaskManager instance = AsyncTaskManager.getInstance(getProject());
        AbstractAsyncTask task = instance.get(AsyncTaskManager.ASYNC_ACCELERATION_TASK);
        return ((AsyncAccelerationTask) task).isAlreadyRunning();
    }

    private static synchronized QueryHistoryMetaUpdateScheduler getInstanceByProject(String project) {
        return INSTANCE_MAP.get(project);
    }

    public class QueryHistoryMetaUpdateRunner extends QueryHistoryTask {

        @Override
        protected String name() {
            return "metaUpdate";
        }

        @Override
        protected List<QueryHistory> getQueryHistories(int batchSize) {
            QueryHistoryIdOffsetManager qhIdOffsetManager = QueryHistoryIdOffsetManager
                    .getInstance(project);
            List<QueryHistory> queryHistoryList = queryHistoryDAO.queryQueryHistoriesByIdOffset(
                    qhIdOffsetManager.get(META).getOffset(), batchSize, project);
            resetIdOffset(queryHistoryList);
            return queryHistoryList;
        }

        @Override
        public void work() {
            int maxSize = KylinConfig.getInstanceFromEnv().getQueryHistoryStatMetaUpdateMaxSize();
            int batchSize = KylinConfig.getInstanceFromEnv().getQueryHistoryStatMetaUpdateBatchSize();
            batchHandle(batchSize, maxSize, this::updateStatMeta);
        }

        private void updateStatMeta(List<QueryHistory> queryHistories) {
            long maxId = 0;
            Map<String, Long> modelsLastQueryTime = Maps.newHashMap();
            val dfHitCountMap = collectDataflowHitCount(queryHistories);
            for (QueryHistory queryHistory : queryHistories) {
                collectModelLastQueryTime(queryHistory, modelsLastQueryTime);

                if (queryHistory.getId() > maxId) {
                    maxId = queryHistory.getId();
                }
            }
            // count snapshot hit
            val hitSnapshotCountMap = collectSnapshotHitCount(queryHistories);

            // update metadata
            updateMetadata(dfHitCountMap, modelsLastQueryTime, maxId, hitSnapshotCountMap);
        }

        private void updateMetadata(Map<String, DataflowHitCount> dfHitCountMap, Map<String, Long> modelsLastQueryTime,
                                    Long maxId, Map<TableDesc, Integer> hitSnapshotCountMap) {
            EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                KylinConfig config = KylinConfig.getInstanceFromEnv();

                // update model usage
                incQueryHitCount(dfHitCountMap, project);

                // update model last query time
                updateLastQueryTime(modelsLastQueryTime, project);

                UnitOfWork.get().doAfterUnit(() -> {
                    QueryHistoryIdOffsetManager offsetManager = QueryHistoryIdOffsetManager.getInstance(project);
                    // update id offset
                    JdbcUtil.withTxAndRetry(offsetManager.getTransactionManager(), () -> {
                        QueryHistoryIdOffset queryHistoryIdOffset = offsetManager.get(META);
                        queryHistoryIdOffset.setOffset(maxId);
                        offsetManager.saveOrUpdate(queryHistoryIdOffset);
                        return null;
                    });
                });

                // update snpashot hit count
                incQueryHitSnapshotCount(hitSnapshotCountMap, project);

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
                    if (dfManager.getDataflow(realization.getModelId()) == null || realization.getLayoutId() == null) {
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

        private Map<TableDesc, Integer> collectSnapshotHitCount(List<QueryHistory> queryHistories) {
            val tableManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            val results = Maps.<TableDesc, Integer> newHashMap();
            for (QueryHistory queryHistory : queryHistories) {
                if (queryHistory.getQueryHistoryInfo() == null) {
                    continue;
                }
                val snapshotsInRealization = queryHistory.getQueryHistoryInfo().getQuerySnapshots();
                for (val snapshots : snapshotsInRealization) {
                    snapshots.stream().forEach(tableIdentify -> {
                        results.merge(tableManager.getTableDesc(tableIdentify), 1, Integer::sum);
                    });
                }
            }
            return results;
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

        private void incQueryHitSnapshotCount(Map<TableDesc, Integer> hitSnapshotCountMap, String project) {
            val tableManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            for (val entry : hitSnapshotCountMap.entrySet()) {
                if (tableManager.getTableDesc(entry.getKey().getIdentity()) == null) {
                    continue;
                }
                val tableCopy = tableManager.copyForWrite(entry.getKey());
                tableCopy.setSnapshotHitCount(tableCopy.getSnapshotHitCount() + entry.getValue());
                tableManager.updateTableDesc(tableCopy);
            }
        }

        private void updateLastQueryTime(Map<String, Long> modelsLastQueryTime, String project) {
            NDataflowManager dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            for (Map.Entry<String, Long> entry : modelsLastQueryTime.entrySet()) {
                String dataflowId = entry.getKey();
                Long lastQueryTime = entry.getValue();
                dfManager.updateDataflow(dataflowId, copyForWrite -> copyForWrite.setLastQueryTime(lastQueryTime));
            }
        }

    }

    private abstract class QueryHistoryTask implements Runnable {

        protected abstract String name();

        private volatile boolean needResetOffset = true;

        protected void resetIdOffset(List<QueryHistory> queryHistories) {
            if (needResetOffset && CollectionUtils.isEmpty(queryHistories)) {
                long maxId = queryHistoryDAO.getQueryHistoryMaxId(project);
                resetIdOffset(maxId);
            }
        }

        private void resetIdOffset(long maxId) {
            QueryHistoryIdOffsetManager manager = QueryHistoryIdOffsetManager.getInstance(project);
            JdbcUtil.withTxAndRetry(manager.getTransactionManager(), () -> {
                QueryHistoryIdOffset queryHistoryIdOffset = manager.get(META);
                if (queryHistoryIdOffset.getOffset() > maxId) {
                    queryHistoryIdOffset.setOffset(maxId);
                    queryHistoryIdOffset.setOffset(maxId);
                    manager.saveOrUpdate(queryHistoryIdOffset);
                }
                return null;
            });
            needResetOffset = false;
        }

        public void batchHandle(int batchSize, int maxSize, Consumer<List<QueryHistory>> consumer) {
            if (!(batchSize > 0 && maxSize >= batchSize)) {
                throw new IllegalArgumentException(String.format(Locale.ROOT,
                        "%s task, batch size: %d , maxsize: %d is illegal", name(), batchSize, maxSize));
            }
            if (!KylinConfig.getInstanceFromEnv().isUTEnv()
                    && !EpochManager.getInstance().checkEpochId(epochId, project)) {
                shutdownByProject(project);
                return;
            }
            int finishNum = 0;
            while (true) {
                List<QueryHistory> queryHistories = getQueryHistories(batchSize);
                finishNum = finishNum + queryHistories.size();
                if (isInterrupted()) {
                    break;
                }
                if (!queryHistories.isEmpty()) {
                    consumer.accept(queryHistories);
                }
                log.debug("{} handled {} query history", name(), queryHistories.size());
                if (queryHistories.size() < batchSize || finishNum >= maxSize) {
                    break;
                }
            }
        }

        protected boolean isInterrupted() {
            return false;
        }

        protected abstract List<QueryHistory> getQueryHistories(int batchSize);

        @Override
        public void run() {
            try {
                work();
            } catch (Exception e) {
                log.warn("QueryHistory {}  process failed of project({})", name(), project, e);
            }
        }

        protected abstract void work();

    }

    @Data
    private static class DataflowHitCount {

        Map<Long, FrequencyMap> layoutHits = Maps.newHashMap();

        int dataflowHit;
    }
}
