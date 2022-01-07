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

import java.util.Arrays;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.common.metrics.MetricsCategory;
import io.kyligence.kap.common.metrics.MetricsGroup;
import io.kyligence.kap.common.metrics.MetricsName;
import io.kyligence.kap.common.scheduler.EventBusFactory;
import io.kyligence.kap.common.scheduler.SourceUsageUpdateNotifier;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.val;

public class GarbageCleaner {

    private static final Logger logger = LoggerFactory.getLogger(GarbageCleaner.class);

    private GarbageCleaner() {
    }

    /**
     * Clean up metadata
     * @param project
     */
    public static void cleanMetadata(String project) {
        val projectInstance = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(project);
        if (projectInstance == null) {
            return;
        }

        List<MetadataCleaner> cleaners = initCleaners(project);
        cleaners.forEach(MetadataCleaner::prepare);
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            cleaners.forEach(MetadataCleaner::cleanup);
            return 0;
        }, project);

        EventBusFactory.getInstance().postAsync(new SourceUsageUpdateNotifier());
        MetricsGroup.hostTagCounterInc(MetricsName.METADATA_CLEAN, MetricsCategory.PROJECT, project);
    }

    private static List<MetadataCleaner> initCleaners(String project) {
        return Arrays.asList(new SnapshotCleaner(project), new IndexCleaner(project), new ExecutableCleaner(project));
    }

}
