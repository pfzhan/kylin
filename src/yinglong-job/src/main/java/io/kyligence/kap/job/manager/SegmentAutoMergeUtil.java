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

package io.kyligence.kap.job.manager;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.model.SegmentRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NSegmentConfigHelper;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import lombok.val;

public class SegmentAutoMergeUtil {

    private static Logger logger = LoggerFactory.getLogger(SegmentAutoMergeUtil.class);

    private SegmentAutoMergeUtil() {
    }

    public static void autoMergeSegments(String project, String modelId, String owner) {
        if (SegmentAutoMergeUtil.canSkipMergeAndClearSeg(project, modelId)) {
            return;
        }
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            handleRetention(project, modelId);
            doAutoMerge(project, modelId, owner);
            return null;
        }, project, 1);
    }
    private static void doAutoMerge(String project, String modelId, String owner) {
        val dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val df = dfManager.getDataflow(modelId);
        val segmentConfig = NSegmentConfigHelper.getModelSegmentConfig(project, modelId);
        Preconditions.checkState(segmentConfig != null);
        SegmentRange rangeToMerge = df.getSegments().autoMergeSegments(segmentConfig);

        if (rangeToMerge != null) {
            NDataSegment mergeSeg = null;
            try {
                mergeSeg = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project).mergeSegments(df,
                        rangeToMerge, true);
            } catch (Exception e) {
                logger.warn("Failed to generate a merge segment", e);
            }

            if (mergeSeg != null) {
                JobManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                        .mergeSegmentJob(new JobParam(mergeSeg, modelId, owner));
            }
        }
    }

    private static void handleRetention(String project, String modelId) {
        val dfManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val df = dfManager.getDataflow(modelId);
        dfManager.handleRetention(df);
    }

    public static boolean canSkipMergeAndClearSeg(String project, String modelId) {
        val segmentConfig = NSegmentConfigHelper.getModelSegmentConfig(project, modelId);
        if (segmentConfig == null) {
            logger.error("segment config is null");
            return true;
        }
        return segmentConfig.canSkipAutoMerge() && segmentConfig.canSkipHandleRetentionSegment();
    }
}
