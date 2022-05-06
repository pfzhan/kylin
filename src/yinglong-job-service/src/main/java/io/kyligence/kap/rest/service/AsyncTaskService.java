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
package io.kyligence.kap.rest.service;

import static io.kyligence.kap.rest.service.AsyncTaskQueryHistorySupporter.CSV_UTF8_BOM;
import static io.kyligence.kap.rest.service.AsyncTaskQueryHistorySupporter.DELETED_MODEL;

import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_DOWNLOAD_FILE;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;

import io.kyligence.kap.metadata.query.QueryHistorySql;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;

import io.kyligence.kap.common.metrics.MetricsCategory;
import io.kyligence.kap.common.metrics.MetricsGroup;
import io.kyligence.kap.common.metrics.MetricsName;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.query.NativeQueryRealization;
import io.kyligence.kap.metadata.query.QueryHistory;
import io.kyligence.kap.metadata.query.QueryHistoryDAO;
import io.kyligence.kap.metadata.query.QueryHistoryInfo;
import io.kyligence.kap.metadata.query.QueryHistoryRequest;
import io.kyligence.kap.metadata.query.util.QueryHistoryUtil;
import io.kyligence.kap.tool.garbage.StorageCleaner;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class AsyncTaskService implements AsyncTaskServiceSupporter {

    private static final String GLOBAL = "global";
    @Async
    public void cleanupStorage() throws Exception {

        long startAt = System.currentTimeMillis();
        try {
            val storageCleaner = new StorageCleaner();
            storageCleaner.execute();
        } catch (Exception e) {
            MetricsGroup.hostTagCounterInc(MetricsName.STORAGE_CLEAN_FAILED, MetricsCategory.GLOBAL, GLOBAL);
            throw e;
        } finally {
            MetricsGroup.hostTagCounterInc(MetricsName.STORAGE_CLEAN, MetricsCategory.GLOBAL, GLOBAL);
            MetricsGroup.hostTagCounterInc(MetricsName.STORAGE_CLEAN_DURATION, MetricsCategory.GLOBAL, GLOBAL,
                    System.currentTimeMillis() - startAt);
        }
    }

    @Override
    @Async
    public Future<Long> runDownloadQueryHistory(QueryHistoryRequest request, HttpServletResponse response,
            ZoneOffset zoneOffset, Integer timeZoneOffsetHour, QueryHistoryDAO queryHistoryDao, boolean onlySql) {
        long start = System.currentTimeMillis();
        try (ServletOutputStream ops = response.getOutputStream()) {
            if (!onlySql) {
                ops.write(CSV_UTF8_BOM);
                ops.write(MsgPicker.getMsg().getQUERY_HISTORY_COLUMN_META().getBytes(StandardCharsets.UTF_8));
            }
            batchDownload(request, zoneOffset, timeZoneOffsetHour, queryHistoryDao, onlySql, ops);
        } catch (IOException e) {
            throw new KylinException(FAILED_DOWNLOAD_FILE, e.getMessage());
        }
        return new AsyncResult<>((System.currentTimeMillis() - start) / 1000);
    }

    private void batchDownload(QueryHistoryRequest request, ZoneOffset zoneOffset, Integer timeZoneOffsetHour,
            QueryHistoryDAO queryHistoryDao, boolean onlySql, ServletOutputStream outputStream) throws IOException {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        int needDownload = Math.min((int) queryHistoryDao.getQueryHistoriesSize(request, request.getProject()),
                kylinConfig.getQueryHistoryDownloadMaxSize());
        int hadDownload = 0;
        while (hadDownload < needDownload) {
            int batchSize = Math.min(kylinConfig.getQueryHistoryDownloadBatchSize(), needDownload - hadDownload);
            List<QueryHistory> queryHistories = queryHistoryDao.getQueryHistoriesByConditionsWithOffset(request, batchSize, hadDownload);
            for (QueryHistory queryHistory : queryHistories) {
                fillingModelAlias(kylinConfig, request.getProject(), queryHistory);
                if (onlySql) {
                    QueryHistorySql queryHistorySql = queryHistory.getQueryHistorySql();
                    String sql = queryHistorySql.getNormalizedSql();
                    outputStream.write((sql.replaceAll("\n|\r", " ") + ";\n").getBytes(StandardCharsets.UTF_8));
                } else {
                    outputStream.write((QueryHistoryUtil.getDownloadData(queryHistory, zoneOffset, timeZoneOffsetHour) + "\n").getBytes(StandardCharsets.UTF_8));
                }
            }
            hadDownload = hadDownload + queryHistories.size();
        }
    }

    private void fillingModelAlias(KylinConfig kylinConfig, String project, QueryHistory qh) {
        if (isQueryHistoryInfoEmpty(qh)) {
            return;
        }
        val noBrokenModels = NDataflowManager.getInstance(kylinConfig, project).listUnderliningDataModels().stream()
                .collect(Collectors.toMap(NDataModel::getAlias, RootPersistentEntity::getUuid));
        val dataModelManager = NDataModelManager.getInstance(kylinConfig, project);
        List<NativeQueryRealization> realizations = qh.transformRealizations();

        realizations.forEach(realization -> {
            NDataModel nDataModel = dataModelManager.getDataModelDesc(realization.getModelId());
            if (noBrokenModels.containsValue(realization.getModelId())) {
                realization.setModelAlias(nDataModel.getFusionModelAlias());
            } else {
                val modelAlias = nDataModel == null ? DELETED_MODEL
                        : String.format(Locale.ROOT, "%s broken", nDataModel.getAlias());
                realization.setModelAlias(modelAlias);
            }
        });
        qh.setNativeQueryRealizations(realizations);
    }

    private boolean isQueryHistoryInfoEmpty(QueryHistory queryHistory) {
        QueryHistoryInfo qhInfo = queryHistory.getQueryHistoryInfo();
        return (qhInfo == null || qhInfo.getRealizationMetrics() == null || qhInfo.getRealizationMetrics().isEmpty())
                && StringUtils.isEmpty(queryHistory.getQueryRealizations());
    }
}
