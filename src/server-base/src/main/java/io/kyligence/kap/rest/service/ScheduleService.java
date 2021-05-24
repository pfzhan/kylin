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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.SetThreadName;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import io.kyligence.kap.common.metrics.MetricsCategory;
import io.kyligence.kap.common.metrics.MetricsGroup;
import io.kyligence.kap.common.metrics.MetricsName;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.metadata.epoch.EpochManager;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.metadata.sourceusage.SourceUsageManager;
import io.kyligence.kap.tool.garbage.SourceUsageCleaner;
import io.kyligence.kap.tool.routine.RoutineTool;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class ScheduleService {

    private static final String GLOBAL = "global";

    @Autowired
    MetadataBackupService backupService;

    @Autowired
    ProjectService projectService;

    @Autowired
    RawRecService rawRecService;

    @Scheduled(cron = "${kylin.metadata.ops-cron:0 0 0 * * *}")
    public void routineTask() throws Exception {

        EpochManager epochManager = EpochManager.getInstance(KylinConfig.getInstanceFromEnv());

        MetricsGroup.hostTagCounterInc(MetricsName.METADATA_OPS_CRON, MetricsCategory.GLOBAL, GLOBAL);

        try (SetThreadName ignored = new SetThreadName("RoutineOpsWorker")) {
            log.info("Start to work");
            if (epochManager.checkEpochOwner(EpochManager.GLOBAL)) {
                backupService.backupAll();
                RoutineTool.cleanQueryHistories();
                RoutineTool.cleanStreamingStats();
                RoutineTool.deleteRawRecItems();
                EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                    new SourceUsageCleaner().cleanup();
                    return null;
                }, UnitOfWork.GLOBAL_UNIT);
            }
            projectService.garbageCleanup();

            log.info("Finish to work");
        }

        MetricsGroup.hostTagCounterInc(MetricsName.METADATA_OPS_CRON_SUCCESS, MetricsCategory.GLOBAL, GLOBAL);
    }

    @Scheduled(cron = "${kylin.metadata.top-recs-filter-cron:0 0 0 * * *}")
    public void selectTopRec() {

        MetricsGroup.hostTagCounterInc(MetricsName.METADATA_OPS_CRON, MetricsCategory.GLOBAL, GLOBAL);

        try (SetThreadName ignored = new SetThreadName("UpdateTopNRecommendationsWorker")) {
            log.info("Routine task to update cost and topN recommendations");

            rawRecService.updateCostsAndTopNCandidates(null);

            log.info("Updating cost and topN recommendations finished.");
        }

        MetricsGroup.hostTagCounterInc(MetricsName.METADATA_OPS_CRON_SUCCESS, MetricsCategory.GLOBAL, GLOBAL);
    }

    @Scheduled(cron = "${kylin.metadata.history-source-usage-cron:0 0 0 * * *}")
    public void updateHistorySourceUsage() {
        if (EpochManager.getInstance(KylinConfig.getInstanceFromEnv()).checkEpochOwner(EpochManager.GLOBAL)) {
            log.info("Start to update history source usage.");
            val sourceUsageManager = SourceUsageManager.getInstance(KylinConfig.getInstanceFromEnv());
            val sourceUsageRecord = sourceUsageManager.refreshLatestSourceUsageRecord();

            EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                log.debug("Start to update source usage...");
                SourceUsageManager.getInstance(KylinConfig.getInstanceFromEnv()).updateSourceUsage(sourceUsageRecord);
                return 0;
            }, EpochManager.GLOBAL);
            log.info("Update history source usage finished.");
        }
    }
}
