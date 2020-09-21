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

package io.kyligence.kap.tool.garbage;

import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.common.metrics.MetricsCategory;
import io.kyligence.kap.common.metrics.MetricsGroup;
import io.kyligence.kap.common.metrics.MetricsName;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.sourceusage.SourceUsageManager;
import lombok.val;

public class GarbageCleaner {

    private static final Logger logger = LoggerFactory.getLogger(GarbageCleaner.class);

    private GarbageCleaner() {
    }

    /**
     * Clean up metadata manually
     * Note that this is not a cluster safe method
     * and should only be used in command line tools while KE is shutdown.
     * For a safe metadata clean up,
     * please see {@link io.kyligence.kap.tool.garbage.GarbageCleaner#cleanupMetadataManually(java.lang.String)}
     * @param project
     */
    public static void unsafeCleanupMetadataManually(String project) {
        if (NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(project) == null) {
            return;
        }

        SnapshotCleaner snapshotCleaner = new SnapshotCleaner(project);
        snapshotCleaner.checkStaleSnapshots();

        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            doCleanupMetadataManually(project, snapshotCleaner);
            return 0;
        }, project);

        MetricsGroup.hostTagCounterInc(MetricsName.METADATA_CLEAN, MetricsCategory.PROJECT, project);
    }

    /**
     * trigger by user
     * @param project
     */
    public static void cleanupMetadataManually(String project) {
        SnapshotCleaner snapshotCleaner = new SnapshotCleaner(project);
        snapshotCleaner.checkStaleSnapshots();

        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            doCleanupMetadataManually(project, snapshotCleaner);
            return 0;
        }, project);
        SourceUsageManager sourceUsageManager = SourceUsageManager.getInstance(KylinConfig.getInstanceFromEnv());
        sourceUsageManager.updateSourceUsage();

        MetricsGroup.hostTagCounterInc(MetricsName.METADATA_CLEAN, MetricsCategory.PROJECT, project);
    }

    public static void doCleanupMetadataManually(String project, SnapshotCleaner snapshotCleaner) {
        val instance = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(project);
        if (instance.isSmartMode()) {
            new BrokenModelCleaner().cleanup(project);
        }

        if (!instance.isExpertMode()) {
            new IndexCleaner().cleanup(project);
        }
        new ExecutableCleaner().cleanup(project);
        snapshotCleaner.cleanup(project);
    }

    /**
     * trigger by a scheduler that is scheduled at 12:00 am every day
     * @param project
     */
    public static void cleanupMetadataAtScheduledTime(String project) {
        SnapshotCleaner snapshotCleaner = new SnapshotCleaner(project);
        snapshotCleaner.checkStaleSnapshots();

        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            val instance = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(project);
            if (instance.isSmartMode()) {
                new BrokenModelCleaner().cleanup(project);
            }

            if (!instance.isExpertMode()) {
                new IndexCleaner().cleanup(project);
            }
            new ExecutableCleaner().cleanup(project);
            snapshotCleaner.cleanup(project);
            return 0;
        }, project);
        SourceUsageManager sourceUsageManager = SourceUsageManager.getInstance(KylinConfig.getInstanceFromEnv());
        sourceUsageManager.updateSourceUsage();
        MetricsGroup.hostTagCounterInc(MetricsName.METADATA_CLEAN, MetricsCategory.PROJECT, project);
    }
}
