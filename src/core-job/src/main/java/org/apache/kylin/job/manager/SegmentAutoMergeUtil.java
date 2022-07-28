/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.job.manager;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NSegmentConfigHelper;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

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
