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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import io.kyligence.kap.common.metrics.NMetricsCategory;
import io.kyligence.kap.common.metrics.NMetricsGroup;
import io.kyligence.kap.common.metrics.NMetricsName;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.metadata.epoch.EpochManager;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.tool.garbage.SourceUsageCleaner;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class ScheduleService {

    private static final String GLOBAL = "global";

    @Autowired
    MetadataBackupService backupService;

    @Autowired
    FavoriteQueryService favoriteQueryService;

    @Autowired
    ProjectService projectService;

    @Autowired
    QueryHistoryService queryHistoryService;

    @Autowired
    RawRecService rawRecService;

    @Scheduled(cron = "${kylin.metadata.ops-cron:0 0 0 * * *}")
    public void routineTask() throws Exception {

        EpochManager epochManager = EpochManager.getInstance(KylinConfig.getInstanceFromEnv());

        NMetricsGroup.counterInc(NMetricsName.METADATA_OPS_CRON, NMetricsCategory.GLOBAL, GLOBAL);

        String oldThreadName = Thread.currentThread().getName();
        try {
            Thread.currentThread().setName("RoutineOpsWorker");

            log.info("Start to work");
            if (epochManager.checkEpochOwner(EpochManager.GLOBAL)) {
                backupService.backupAll();
                queryHistoryService.cleanQueryHistories();
                rawRecService.deleteRawRecItems();
                EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                    new SourceUsageCleaner().cleanup();
                    return null;
                }, UnitOfWork.GLOBAL_UNIT);
            }
            projectService.garbageCleanup();

            log.info("Finish to work");
        } finally {
            Thread.currentThread().setName(oldThreadName);
        }

        NMetricsGroup.counterInc(NMetricsName.METADATA_OPS_CRON_SUCCESS, NMetricsCategory.GLOBAL, GLOBAL);
    }

    @Scheduled(cron = "${kylin.metadata.top-recs-filter-cron:0 0 0 * * *}")
    public void selectTopRec() {

        NMetricsGroup.counterInc(NMetricsName.METADATA_OPS_CRON, NMetricsCategory.GLOBAL, GLOBAL);

        String oldThreadName = Thread.currentThread().getName();
        try {
            Thread.currentThread().setName("SelectTopRecommendations");
            log.info("Routine task to update cost and select topN recommendations");

            rawRecService.updateCostAndSelectTopRec();

            log.info("Updating cost and selecting topN recommendations finished.");
        } finally {
            Thread.currentThread().setName(oldThreadName);
        }

        NMetricsGroup.counterInc(NMetricsName.METADATA_OPS_CRON_SUCCESS, NMetricsCategory.GLOBAL, GLOBAL);
    }
}
