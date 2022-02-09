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

import io.kyligence.kap.metadata.epoch.EpochManager;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.metadata.sourceusage.SourceUsageManager;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class SourceUsageScheduleService {

    @Autowired
    SourceUsageService sourceUsageService;

    @Scheduled(cron = "${kylin.metadata.history-source-usage-cron:0 0 0 * * *}")
    public void updateHistorySourceUsage() {
        if (EpochManager.getInstance().checkEpochOwner(EpochManager.GLOBAL)) {
            log.info("Start to update history source usage.");
            val sourceUsageRecord = sourceUsageService.refreshLatestSourceUsageRecord();

            EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                log.debug("Start to update source usage...");
                SourceUsageManager.getInstance(KylinConfig.getInstanceFromEnv()).updateSourceUsage(sourceUsageRecord);
                return 0;
            }, EpochManager.GLOBAL);
            log.info("Update history source usage finished.");
        }
    }
}
