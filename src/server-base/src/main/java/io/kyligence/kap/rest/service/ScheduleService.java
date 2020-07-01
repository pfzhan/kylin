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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import io.kyligence.kap.common.metrics.NMetricsCategory;
import io.kyligence.kap.common.metrics.NMetricsGroup;
import io.kyligence.kap.common.metrics.NMetricsName;
import io.kyligence.kap.metadata.epoch.EpochManager;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class ScheduleService {

    private static final Logger logger = LoggerFactory.getLogger(ScheduleService.class);

    @Autowired
    MetadataBackupService backupService;

    @Autowired
    FavoriteQueryService favoriteQueryService;

    @Autowired
    ProjectService projectService;

    @Autowired
    QueryHistoryService queryHistoryService;

    @Autowired
    RawRecommendationService rawRecommendationService;

    @Scheduled(cron = "${kylin.metadata.ops-cron:0 0 0 * * *}")
    public void routineTask() throws Exception {

        EpochManager epochManager = EpochManager.getInstance(KylinConfig.getInstanceFromEnv());

        NMetricsGroup.counterInc(NMetricsName.METADATA_OPS_CRON, NMetricsCategory.GLOBAL, "global");

        String oldThreadName = Thread.currentThread().getName();
        try {
            Thread.currentThread().setName("RoutineOpsWorker");

            logger.info("Start to work");
            if (epochManager.checkEpochOwner(EpochManager.GLOBAL)) {
                backupService.backupAll();
            }
            projectService.garbageCleanup();
            favoriteQueryService.adjustFalseAcceleratedFQ();
            favoriteQueryService.generateRecommendation();
            queryHistoryService.cleanQueryHistories();
            rawRecommendationService.updateCostAndSelectTopRec();

            logger.info("Finish to work");
        } finally {
            Thread.currentThread().setName(oldThreadName);
        }

        NMetricsGroup.counterInc(NMetricsName.METADATA_OPS_CRON_SUCCESS, NMetricsCategory.GLOBAL, "global");
    }
}
