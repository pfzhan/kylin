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
package org.apache.kylin.job.runners;

import com.google.common.collect.Lists;
import io.kyligence.kap.metadata.cube.storage.ProjectStorageInfoCollector;
import io.kyligence.kap.metadata.cube.storage.StorageInfoEnum;
import lombok.val;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QuotaStorageCheckRunner extends AbstractDefaultSchedulerRunner {
    private static final Logger logger = LoggerFactory.getLogger(QuotaStorageCheckRunner.class);

    private final ProjectStorageInfoCollector collector;

    public QuotaStorageCheckRunner(NDefaultScheduler nDefaultScheduler) {
        super(nDefaultScheduler);

        val storageInfoEnumList = Lists.newArrayList(StorageInfoEnum.STORAGE_QUOTA, StorageInfoEnum.TOTAL_STORAGE);
        collector = new ProjectStorageInfoCollector(storageInfoEnumList);
    }

    @Override
    protected void doRun() {
        logger.info("start check project {} storage quota.", nDefaultScheduler.getProject());
        context.setReachQuotaLimit(reachStorageQuota());
    }

    private boolean reachStorageQuota() {
        val storageVolumeInfo = collector.getStorageVolumeInfo(KylinConfig.getInstanceFromEnv(), nDefaultScheduler.getProject());
        val totalSize = storageVolumeInfo.getTotalStorageSize();
        val storageQuotaSize = storageVolumeInfo.getStorageQuotaSize();
        if (totalSize < 0) {
            logger.error(
                    "Project '{}' : an exception occurs when getting storage volume info, no job will be scheduled!!! The error info : {}",
                    nDefaultScheduler.getProject(), storageVolumeInfo.getThrowableMap().get(StorageInfoEnum.TOTAL_STORAGE));
            return true;
        }
        if (totalSize >= storageQuotaSize) {
            logger.info("Project '{}' reach storage quota, no job will be scheduled!!!", nDefaultScheduler.getProject());
            return true;
        }
        return false;
    }
}
