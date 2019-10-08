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

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.common.metrics.NMetricsCategory;
import io.kyligence.kap.common.metrics.NMetricsGroup;
import io.kyligence.kap.common.metrics.NMetricsName;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.metadata.cube.garbage.FrequencyMap;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.favorite.FavoriteQuery;
import io.kyligence.kap.metadata.favorite.FavoriteQueryManager;
import io.kyligence.kap.metadata.favorite.QueryHistoryTimeOffset;
import io.kyligence.kap.metadata.favorite.QueryHistoryTimeOffsetManager;
import io.kyligence.kap.metadata.query.QueryHistory;
import lombok.Data;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UpdateUsageStatisticsRunner implements Runnable {

    private final String project;
    private final QueryHistoryAccessor queryHistoryAccessor;

    public UpdateUsageStatisticsRunner(String project) {
        this.project = project;
        queryHistoryAccessor = new QueryHistoryAccessor(project);
    }

    @Override
    public void run() {
        long startTime = System.currentTimeMillis();
        try {
            updateFavoriteStatistics();
            log.info("update favorite stats runner takes {}ms", System.currentTimeMillis() - startTime);
        } catch (Exception ex) {
            NMetricsGroup.counterInc(NMetricsName.FQ_FAILED_UPDATE_USAGE, NMetricsCategory.PROJECT, project);
            log.error("Error {} caught when updating favorite queries for project {}", ex.getMessage(), project);
        } finally {
            NMetricsGroup.counterInc(NMetricsName.FQ_UPDATE_USAGE, NMetricsCategory.PROJECT, project);
            NMetricsGroup.counterInc(NMetricsName.FQ_UPDATE_USAGE_DURATION, NMetricsCategory.PROJECT, project,
                    System.currentTimeMillis() - startTime);
        }
    }

    private void updateFavoriteStatistics() {
        QueryHistoryTimeOffset timeOffset = QueryHistoryTimeOffsetManager
                .getInstance(KylinConfig.getInstanceFromEnv(), project).get();
        long startTime = timeOffset.getFavoriteQueryUpdateTimeOffset();
        long endTime = startTime + queryHistoryAccessor.getFetchQueryHistoryGapTime();
        long backwardShiftTime = KapConfig.getInstanceFromEnv().getInfluxDBFlushDuration() * 2L;

        long maxTime = queryHistoryAccessor.getSystemTime() - backwardShiftTime;

        while (endTime <= maxTime) {
            val queryHistories = queryHistoryAccessor.getQueryHistoryDao().getQueryHistoriesByTime(startTime, endTime);

            if (CollectionUtils.isEmpty(queryHistories)) {
                endTime = queryHistoryAccessor.skipEmptyIntervals(endTime, maxTime);
            }

            updateRelatedMetadata(queryHistories, endTime);

            startTime = endTime;
            endTime = startTime + queryHistoryAccessor.getFetchQueryHistoryGapTime();
        }

        if (startTime < maxTime) {
            val queryHistories = queryHistoryAccessor.getQueryHistoryDao().getQueryHistoriesByTime(startTime, maxTime);
            updateRelatedMetadata(queryHistories, maxTime);
        }
    }

    private void updateRelatedMetadata(List<QueryHistory> queryHistories, long scannedOffset) {
        Map<String, FavoriteQuery> favoritesAboutToUpdate = Maps.newHashMap();

        val dfHitCountMap = collectDataflowHitCount(queryHistories);
        for (QueryHistory queryHistory : queryHistories) {
            updateFavoriteQuery(queryHistory, favoritesAboutToUpdate);
        }

        List<FavoriteQuery> favoriteQueries = Lists.newArrayList();
        for (Map.Entry<String, FavoriteQuery> favoritesInProj : favoritesAboutToUpdate.entrySet()) {
            favoriteQueries.add(favoritesInProj.getValue());
        }

        UnitOfWork.doInTransactionWithRetry(() -> {
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            // update model usage
            incQueryHitCount(dfHitCountMap);
            // update favorite query statistics
            FavoriteQueryManager.getInstance(config, project).updateStatistics(favoriteQueries);
            QueryHistoryTimeOffset timeOffset = QueryHistoryTimeOffsetManager
                    .getInstance(KylinConfig.getInstanceFromEnv(), project).get();
            timeOffset.setFavoriteQueryUpdateTimeOffset(scannedOffset);
            QueryHistoryTimeOffsetManager.getInstance(config, project).save(timeOffset);
            return 0;
        }, project);
    }

    private void incQueryHitCount(Map<String, DataflowHitCount> dfHitCountMap) {
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

    private void updateFavoriteQuery(QueryHistory queryHistory, Map<String, FavoriteQuery> favoritesAboutToUpdate) {
        String sqlPattern = queryHistory.getSqlPattern();

        if (!FavoriteQueryManager.getInstance(KylinConfig.getInstanceFromEnv(), project).contains(sqlPattern))
            return;

        FavoriteQuery favoriteQuery = favoritesAboutToUpdate.get(sqlPattern);
        if (favoriteQuery == null) {
            favoriteQuery = new FavoriteQuery(sqlPattern);
        }

        favoriteQuery.incStats(queryHistory);
        favoritesAboutToUpdate.put(sqlPattern, favoriteQuery);
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

    @Data
    static class DataflowHitCount {

        Map<Long, FrequencyMap> layoutHits = Maps.newHashMap();

        int dataflowHit;
    }

}
