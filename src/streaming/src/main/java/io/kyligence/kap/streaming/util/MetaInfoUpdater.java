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

package io.kyligence.kap.streaming.util;

import io.kyligence.kap.guava20.shaded.common.collect.Sets;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.streaming.manager.StreamingJobManager;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Set;

public class MetaInfoUpdater {
    private static final Logger logger = LoggerFactory.getLogger(MetaInfoUpdater.class);

    public static void update(String project, NDataSegment seg, NDataLayout layout) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NDataflowUpdate update = new NDataflowUpdate(seg.getDataflow().getUuid());
            update.setToAddOrUpdateLayouts(layout);
            NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project).updateDataflow(update);
            return 0;
        }, project);
    }

    public static void updateJobState(String project, String jobId, JobStatusEnum state) {
        updateJobState(project, jobId, state, null);
    }

    public static void updateJobState(String project, String jobId, JobStatusEnum state, String errorMsg) {
        updateJobState(project, jobId, Sets.newHashSet(state), state, errorMsg);
    }

    public static void updateJobState(String project, String jobId, Set<JobStatusEnum> excludeStates,
            JobStatusEnum state, String errorMsg) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            StreamingJobManager mgr = StreamingJobManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            mgr.updateStreamingJob(jobId, copyForWrite -> {
                if (!excludeStates.contains(copyForWrite.getCurrentStatus())) {
                    copyForWrite.setCurrentStatus(state);
                    if (errorMsg != null) {
                        logger.warn(errorMsg, jobId);
                    }
                    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss",
                            Locale.getDefault(Locale.Category.FORMAT));
                    Date date = new Date(System.currentTimeMillis());
                    copyForWrite.setLastUpdateTime(format.format(date));
                    switch (state) {
                    case RUNNING:
                        copyForWrite.setLastStartTime(format.format(date));
                        break;
                    case STOPPED:
                    case ERROR:
                        copyForWrite.setLastEndTime(format.format(date));
                        break;
                    default:
                    }
                }
            });
            return null;
        }, project);
    }
}
